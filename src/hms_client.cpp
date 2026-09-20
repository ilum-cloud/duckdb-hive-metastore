#include "hms_client.hpp"
#include "duckdb/common/exception.hpp"
#include <cstdio>

namespace duckdb {

HMSClient::HMSClient(const string &host, int port) : host(host), port(port), connected(false) {
	socket = std::make_shared<apache::thrift::transport::TSocket>(host, port);
	transport = std::make_shared<apache::thrift::transport::TBufferedTransport>(socket);
	protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
	client = unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>(
	    new Apache::Hadoop::Hive::ThriftHiveMetastoreClient(protocol));
}

HMSClient::~HMSClient() {
	Close();
}

void HMSClient::Open() {
	if (!connected) {
		try {
			transport->open();
			connected = true;
		} catch (apache::thrift::TException &tx) {
			throw IOException("Failed to connect to Hive Metastore at %s:%d - %s", host, port, tx.what());
		}
	}
}

void HMSClient::Close() {
	if (connected) {
		try {
			transport->close();
			connected = false;
		} catch (apache::thrift::TException &) {
			// Silently ignore errors during close - we're likely in a destructor
			// and there's no good way to handle this without risking double-throw
			connected = false;
		}
	}
}

bool HMSClient::IsConnected() const {
	return connected;
}

vector<string> HMSClient::GetAllDatabases() {
	if (!connected)
		Open();
	vector<string> dbs;
	try {
		client->get_all_databases(dbs);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get all databases: %s", tx.what());
	}
	return dbs;
}

vector<string> HMSClient::GetAllTables(const string &db_name) {
	if (!connected)
		Open();
	vector<string> tables;
	try {
		client->get_all_tables(tables, db_name);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get all tables for database '%s': %s", db_name, tx.what());
	}
	return tables;
}

Apache::Hadoop::Hive::Database HMSClient::GetDatabase(const string &db_name) {
	if (!connected)
		Open();
	Apache::Hadoop::Hive::Database db;
	try {
		client->get_database(db, db_name);
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &e) {
		throw IOException("Database '%s' not found: %s", db_name, e.message);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get database '%s': %s", db_name, tx.what());
	}
	return db;
}

bool HMSClient::TryGetTable(const string &db_name, const string &table_name, Apache::Hadoop::Hive::Table &result) {
	if (!connected)
		Open();
	try {
		client->get_table(result, db_name, table_name);
		return true;
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &) {
		return false;
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get table '%s.%s': %s", db_name, table_name, tx.what());
	}
}

vector<Apache::Hadoop::Hive::TableMeta> HMSClient::GetTableMeta(const string &db_patterns,
                                                                const string &table_patterns) {
	if (!connected)
		Open();
	vector<Apache::Hadoop::Hive::TableMeta> tables;
	try {
		client->get_table_meta(tables, db_patterns, table_patterns, vector<string>());
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to list tables matching '%s.%s': %s", db_patterns, table_patterns, tx.what());
	}
	return tables;
}

vector<Apache::Hadoop::Hive::Table> HMSClient::GetTableObjects(const string &db_name,
                                                               const vector<string> &table_names) {
	if (!connected)
		Open();
	vector<Apache::Hadoop::Hive::Table> tables;
	try {
		client->get_table_objects_by_name(tables, db_name, table_names);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get table objects for database '%s': %s", db_name, tx.what());
	}
	return tables;
}

vector<string> HMSClient::GetPartitionNames(const string &db_name, const string &table_name) {
	if (!connected)
		Open();
	vector<string> partition_names;
	try {
		// -1: no limit. Names are cheap; the storage descriptors are fetched separately per batch.
		client->get_partition_names(partition_names, db_name, table_name, -1);
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &) {
		return partition_names;
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to list the partitions of '%s.%s': %s", db_name, table_name, tx.what());
	}
	return partition_names;
}

vector<Apache::Hadoop::Hive::Partition> HMSClient::GetPartitionsByNames(const string &db_name, const string &table_name,
                                                                        const vector<string> &partition_names) {
	if (!connected)
		Open();
	vector<Apache::Hadoop::Hive::Partition> partitions;
	try {
		client->get_partitions_by_names(partitions, db_name, table_name, partition_names);
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &) {
		return partitions;
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get the partitions of '%s.%s': %s", db_name, table_name, tx.what());
	}
	return partitions;
}

void HMSClient::CreateTable(const Apache::Hadoop::Hive::Table &table) {
	if (!connected)
		Open();
	try {
#ifdef THRIFT_HAS_CREATE_TABLE_WITH_ENV
		Apache::Hadoop::Hive::EnvironmentContext env;
		client->create_table_with_environment_context(table, env);
#else
		client->create_table(table);
#endif
	} catch (Apache::Hadoop::Hive::AlreadyExistsException &e) {
		throw IOException("Table '%s.%s' already exists: %s", table.dbName, table.tableName, e.message);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to create table '%s.%s': %s", table.dbName, table.tableName, tx.what());
	}
}

bool HMSClient::DropTable(const string &db_name, const string &table_name, bool delete_data) {
	if (!connected)
		Open();
	try {
		client->drop_table(db_name, table_name, delete_data);
		return true;
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &) {
		// Distinct return value lets the caller honor IF EXISTS without swallowing
		// transport or auth errors that look the same from a generic catch.
		return false;
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to drop table '%s.%s': %s", db_name, table_name, tx.what());
	}
}

} // namespace duckdb

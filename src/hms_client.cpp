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

Apache::Hadoop::Hive::Table HMSClient::GetTable(const string &db_name, const string &table_name) {
	if (!connected)
		Open();
	Apache::Hadoop::Hive::Table table;
	try {
		client->get_table(table, db_name, table_name);
	} catch (Apache::Hadoop::Hive::NoSuchObjectException &e) {
		throw IOException("Table '%s.%s' not found: %s", db_name, table_name, e.message);
	} catch (apache::thrift::TException &tx) {
		throw IOException("Failed to get table '%s.%s': %s", db_name, table_name, tx.what());
	}
	return table;
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

} // namespace duckdb

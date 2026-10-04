#pragma once

#include <string>
#include <vector>
#include <memory>
#include "duckdb/common/common.hpp"

// Thrift headers
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

// Generated Thrift headers
#include "ThriftHiveMetastore.h"

namespace duckdb {

class HMSClient {
public:
	HMSClient(const string &host, int port);
	~HMSClient();

	void Open();
	void Close();
	bool IsConnected() const;

	vector<string> GetAllDatabases();
	vector<string> GetAllTables(const string &db_name);

	Apache::Hadoop::Hive::Database GetDatabase(const string &db_name);
	// Fetch one database. Returns false if it does not exist; other Thrift errors are re-thrown so callers can
	// distinguish "missing" from "broken".
	bool TryGetDatabase(const string &db_name, Apache::Hadoop::Hive::Database &result);
	// Fetch one table. Returns false if the table does not exist; other Thrift errors are re-thrown so callers can
	// distinguish "missing" from "broken".
	bool TryGetTable(const string &db_name, const string &table_name, Apache::Hadoop::Hive::Table &result);
	vector<Apache::Hadoop::Hive::Table> GetTableObjects(const string &db_name, const vector<string> &table_names);
	// Names and types of the tables matching the patterns, across databases, in one call
	vector<Apache::Hadoop::Hive::TableMeta> GetTableMeta(const string &db_patterns, const string &table_patterns);
	// Names ("k=v/k=v") of all partitions of a table; empty if the table has none
	vector<string> GetPartitionNames(const string &db_name, const string &table_name, int16_t max_parts = -1);
	// The partitions a metastore filter selects. Returns false when the metastore refuses the filter (it cannot
	// evaluate it, or the table is gone), with its reason in `rejection`; throws IOException if it cannot be reached.
	bool TryGetPartitionsByFilter(const string &db_name, const string &table_name, const string &filter,
	                              vector<Apache::Hadoop::Hive::Partition> &result, string &rejection);
	// How many partitions a table has
	int32_t CountPartitions(const string &db_name, const string &table_name);
	// The first partitions of a table, at most `max_parts`, each with its storage descriptor
	vector<Apache::Hadoop::Hive::Partition> GetPartitions(const string &db_name, const string &table_name,
	                                                      int16_t max_parts);
	// The named partitions, each with its own storage descriptor (and therefore its own location)
	vector<Apache::Hadoop::Hive::Partition> GetPartitionsByNames(const string &db_name, const string &table_name,
	                                                             const vector<string> &partition_names);
	// Create a table in the metastore using a Thrift Table object
	void CreateTable(const Apache::Hadoop::Hive::Table &table);
	// Drop a table from the metastore. delete_data=false preserves the underlying
	// storage files (we treat HMS as a pure catalog and do not own the bucket layout).
	// Returns true if the table was dropped, false if it did not exist. Other Thrift
	// errors (transport, auth, MetaException) are re-thrown so callers can distinguish
	// "missing" from "broken" — needed for IF EXISTS to behave correctly.
	bool DropTable(const string &db_name, const string &table_name, bool delete_data);

private:
	string host;
	int port;
	bool connected;

	std::shared_ptr<apache::thrift::transport::TSocket> socket;
	std::shared_ptr<apache::thrift::transport::TTransport> transport;
	std::shared_ptr<apache::thrift::protocol::TProtocol> protocol;
	std::unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client;
};

} // namespace duckdb

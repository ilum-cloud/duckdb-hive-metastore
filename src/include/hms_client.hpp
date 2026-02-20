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
	Apache::Hadoop::Hive::Table GetTable(const string &db_name, const string &table_name);
	vector<Apache::Hadoop::Hive::Table> GetTableObjects(const string &db_name, const vector<string> &table_names);
	// Create a table in the metastore using a Thrift Table object
	void CreateTable(const Apache::Hadoop::Hive::Table &table);

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

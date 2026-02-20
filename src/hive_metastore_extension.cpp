#define DUCKDB_EXTENSION_MAIN

#include "hive_metastore_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void HiveMetastoreScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "HiveMetastore " + name.GetString() + " 🐥");
	});
}

inline void HiveMetastoreOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "HiveMetastore " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto hive_metastore_scalar_function = ScalarFunction("hive_metastore", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HiveMetastoreScalarFun);
	loader.RegisterFunction(hive_metastore_scalar_function);

	// Register another scalar function
	auto hive_metastore_openssl_version_scalar_function = ScalarFunction("hive_metastore_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, HiveMetastoreOpenSSLVersionScalarFun);
	loader.RegisterFunction(hive_metastore_openssl_version_scalar_function);
}

void HiveMetastoreExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HiveMetastoreExtension::Name() {
	return "hive_metastore";
}

std::string HiveMetastoreExtension::Version() const {
#ifdef EXT_VERSION_HIVE_METASTORE
	return EXT_VERSION_HIVE_METASTORE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hive_metastore, loader) {
	duckdb::LoadInternal(loader);
}
}

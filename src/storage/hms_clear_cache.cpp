#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "storage/hms_catalog.hpp"
#include "storage/hms_transaction.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ClearCacheFunctionData>();
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void ClearHMSCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		if (!db_ref) {
			continue;
		}
		auto &catalog = db_ref.get()->GetCatalog();
		if (catalog.GetCatalogType() != "hms" && catalog.GetCatalogType() != "hive_metastore") {
			continue;
		}
		catalog.Cast<HMSCatalog>().ClearCache();
		// Also forget what the calling transaction already resolved, so its next lookups see fresh metadata
		auto transaction = Transaction::TryGet(context, *db_ref);
		if (transaction) {
			transaction->Cast<HMSTransaction>().ClearSnapshot();
		}
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	ClearHMSCaches(context);
	data.finished = true;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
}

void HMSClearCacheFunction::ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter) {
	ClearHMSCaches(context);
}

HMSClearCacheFunction::HMSClearCacheFunction()
    : TableFunction("hms_clear_cache", {}, ClearCacheFunction, ClearCacheBind) {
}
} // namespace duckdb

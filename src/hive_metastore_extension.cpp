#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "storage/hms_catalog.hpp"
#include "storage/hms_transaction_manager.hpp"
#include "hive_metastore_extension.hpp"

namespace duckdb {

static unique_ptr<Catalog> HMSCatalogAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	string default_schema;
	string warehouse_location;
	idx_t metadata_cache_ttl_seconds = HMSCatalog::DEFAULT_METADATA_CACHE_TTL_SECONDS;
	auto partition_mode = HMSPartitionMode::AUTO;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "type" || lower_name == "read_only") {
			// already handled
		} else if (lower_name == "default_schema") {
			default_schema = entry.second.ToString();
		} else if (lower_name == "warehouse_location") {
			warehouse_location = entry.second.ToString();
		} else if (lower_name == "metadata_cache_ttl") {
			auto seconds = entry.second.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
			if (seconds < 0) {
				throw BinderException("METADATA_CACHE_TTL must be a number of seconds >= 0, got %d", seconds);
			}
			metadata_cache_ttl_seconds = UnsafeNumericCast<idx_t>(seconds);
		} else if (lower_name == "partition_mode") {
			auto mode = StringUtil::Lower(entry.second.ToString());
			if (mode == "auto") {
				partition_mode = HMSPartitionMode::AUTO;
			} else if (mode == "hms") {
				partition_mode = HMSPartitionMode::HMS;
			} else if (mode == "path") {
				partition_mode = HMSPartitionMode::PATH;
			} else {
				throw BinderException("PARTITION_MODE must be one of 'auto', 'hms', 'path', got: %s", mode);
			}
		} else {
			throw BinderException("Unrecognized option for HMS attach: %s", entry.first);
		}
	}

	if (default_schema.empty()) {
		default_schema = "default";
	}

	string catalog_name = "hive_metastore";
	return make_uniq<HMSCatalog>(db, info.path, attach_options, info.path, default_schema, warehouse_location,
	                             catalog_name, metadata_cache_ttl_seconds, partition_mode);
}

static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                               AttachedDatabase &db, Catalog &catalog) {
	auto &hms_catalog = catalog.Cast<HMSCatalog>();
	return make_uniq<HMSTransactionManager>(db, hms_catalog);
}

class HiveMetastoreStorageExtension : public StorageExtension {
public:
	HiveMetastoreStorageExtension() {
		attach = HMSCatalogAttach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	StorageExtension::Register(config, "hive_metastore", make_uniq<HiveMetastoreStorageExtension>());
	StorageExtension::Register(config, "hms_catalog", make_uniq<HiveMetastoreStorageExtension>());
	loader.RegisterFunction(HMSClearCacheFunction());
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

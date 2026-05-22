#include "storage/hms_catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/hms_schema_entry.hpp"
#include "storage/hms_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include "storage/hms_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "hms_constants.hpp"
#include "hms_format_detector.hpp"
#include "hms_path_utils.hpp"

namespace duckdb {

HMSCatalog::HMSCatalog(AttachedDatabase &db_p, const string &internal_name, AttachOptions &attach_options,
                       string endpoint_p, const string &default_schema, const string &warehouse_location,
                       string catalog_name_p)
    : Catalog(db_p), internal_name(internal_name), access_mode(attach_options.access_mode),
      endpoint(std::move(endpoint_p)), warehouse_location(warehouse_location), schemas(*this),
      default_schema(default_schema), catalog_name(std::move(catalog_name_p)) {
}

HMSCatalog::~HMSCatalog() = default;

void HMSCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> HMSCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropEntry(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void HMSCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void HMSCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<HMSSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> HMSCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA && default_schema != DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException(
			    "Default schema for catalog '%s' not found. This means auto-detection of default schema failed. Please "
			    "specify a DEFAULT_SCHEMA on ATTACH: `ATTACH '..' (TYPE hive_metastore, DEFAULT_SCHEMA 'my_schema')`",
			    GetName());
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (!entry) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw BinderException("Schema with name \"%s\" not found", schema_lookup.GetEntryName());
		}
		return nullptr;
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

bool HMSCatalog::InMemory() {
	return false;
}

string HMSCatalog::GetDBPath() {
	return internal_name;
}

string HMSCatalog::GetDefaultSchema() const {
	return default_schema;
}

DatabaseSize HMSCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

void HMSCatalog::ClearCache() {
	schemas.ClearEntries();
}

// Extract a `WITH (key='value', key=number, key=true|false)` clause from the original
// CREATE TABLE SQL text and populate `tags`. DuckDB 1.4's CREATE TABLE transformer
// silently drops `stmt.options`, so the only path that survives to the extension is
// the raw SQL string on CreateInfo::sql. Handles single-quoted strings (with '' escapes),
// integer / float literals, and unquoted true/false. Identifier keys are lowercased.
// Idempotent: if `tags` is non-empty (e.g. a future DuckDB version starts populating it),
// this is a no-op.
static void ParseWithClauseIntoTags(const string &sql, InsertionOrderPreservingMap<string> &tags) {
	if (sql.empty() || !tags.empty()) {
		return;
	}
	string lower = StringUtil::Lower(sql);
	idx_t search_from = 0;
	while (true) {
		auto with_pos = lower.find("with", search_from);
		if (with_pos == string::npos) {
			return;
		}
		search_from = with_pos + 4;
		// Word-boundary check on both sides.
		if (with_pos > 0 && (std::isalnum(static_cast<unsigned char>(lower[with_pos - 1])) || lower[with_pos - 1] == '_')) {
			continue;
		}
		idx_t p = with_pos + 4;
		while (p < lower.size() && std::isspace(static_cast<unsigned char>(lower[p]))) {
			p++;
		}
		if (p >= lower.size() || lower[p] != '(') {
			continue;
		}
		idx_t body_start = p + 1;
		// Find matching ')' respecting single-quoted strings only.
		idx_t cursor = body_start;
		bool in_quote = false;
		while (cursor < sql.size()) {
			char c = sql[cursor];
			if (in_quote) {
				if (c == '\'' && cursor + 1 < sql.size() && sql[cursor + 1] == '\'') {
					cursor += 2;
					continue;
				}
				if (c == '\'') {
					in_quote = false;
				}
			} else {
				if (c == '\'') {
					in_quote = true;
				} else if (c == ')') {
					break;
				}
			}
			cursor++;
		}
		if (cursor >= sql.size()) {
			return;
		}
		string body = sql.substr(body_start, cursor - body_start);

		// Tokenize by top-level commas (single-quote aware).
		vector<string> pairs;
		string current;
		in_quote = false;
		for (idx_t i = 0; i < body.size(); i++) {
			char c = body[i];
			if (in_quote) {
				current += c;
				if (c == '\'' && i + 1 < body.size() && body[i + 1] == '\'') {
					current += body[++i];
					continue;
				}
				if (c == '\'') {
					in_quote = false;
				}
			} else if (c == '\'') {
				current += c;
				in_quote = true;
			} else if (c == ',') {
				pairs.push_back(std::move(current));
				current.clear();
			} else {
				current += c;
			}
		}
		if (!current.empty()) {
			pairs.push_back(std::move(current));
		}

		for (auto &pair : pairs) {
			auto eq = pair.find('=');
			if (eq == string::npos) {
				continue;
			}
			string key = pair.substr(0, eq);
			string raw_val = pair.substr(eq + 1);
			StringUtil::Trim(key);
			StringUtil::Trim(raw_val);
			if (key.empty() || raw_val.empty()) {
				continue;
			}
			// Strip surrounding quotes if any (identifier-quoted or string-literal).
			string val;
			if (raw_val.size() >= 2 && raw_val.front() == '\'' && raw_val.back() == '\'') {
				val = raw_val.substr(1, raw_val.size() - 2);
				// Collapse doubled single quotes.
				val = StringUtil::Replace(val, "''", "'");
			} else {
				val = raw_val;
			}
			// Strip surrounding double quotes on keys (identifier form).
			if (key.size() >= 2 && key.front() == '"' && key.back() == '"') {
				key = key.substr(1, key.size() - 2);
			}
			tags[StringUtil::Lower(key)] = val;
		}
		return;
	}
}

ErrorData HMSCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	auto &base = info.Base();
	// DuckDB 1.4 does not parse PARTITIONED BY / SORTED BY on plain CREATE TABLE,
	// and the CREATE TABLE transformer drops `stmt.options`. The original SQL is
	// still available on CreateInfo::sql; recover the WITH clause from there so
	// HMSTableSet::CreateTable can read location / format / external_table tags.
	ParseWithClauseIntoTags(base.sql, base.tags);
	return ErrorData();
}

static string JoinDirAndFile(const string &dir, const string &file) {
	if (dir.empty()) {
		return file;
	}
	if (dir.back() == '/') {
		return dir + file;
	}
	return dir + "/" + file;
}

// Build a CopyFunction-backed PhysicalCopyToFile that writes one file per INSERT into the
// HMS table's storage_location. Used by both PlanInsert and PlanCreateTableAs.
static PhysicalOperator &PlanHMSWrite(ClientContext &context, PhysicalPlanGenerator &planner, HMSTableEntry &hms_table,
                                      PhysicalOperator &child_plan, const vector<LogicalType> &output_types,
                                      idx_t estimated_cardinality) {
	if (!hms_table.table_data) {
		throw InternalException("HMS table '%s' has no metadata", hms_table.name);
	}
	if (!hms_table.table_data->partition_keys.empty()) {
		throw NotImplementedException(
		    "INSERT into partitioned HMS tables is not yet supported (table '%s'). Drop partition keys or use a "
		    "non-partitioned table.",
		    hms_table.name);
	}
	// Refuse writes when the HMS-stored location is empty: `JoinDirAndFile("", "data_<uuid>.…")`
	// would yield a relative filename and INSERT would silently write to the DuckDB process cwd.
	if (hms_table.table_data->storage_location.empty()) {
		throw IOException("HMS table '%s' has no storage_location set in the metastore; refusing to write",
		                  hms_table.name);
	}

	auto format_result = hms::FormatDetector::Detect(*hms_table.table_data);
	string format_name;
	if (format_result.format == hms::TableFormat::PARQUET) {
		format_name = hms::format::PARQUET;
	} else if (format_result.format == hms::TableFormat::CSV) {
		format_name = hms::format::CSV;
	} else {
		throw NotImplementedException(
		    "INSERT is not yet supported for the storage format of HMS table '%s' (input_format=%s, serde=%s). Only "
		    "non-partitioned Parquet and CSV are currently writable.",
		    hms_table.name, hms_table.table_data->input_format, hms_table.table_data->serialization_lib);
	}

	// Look up the COPY function in the system catalog. THROW_EXCEPTION here makes the error
	// surface as "Catalog Error: Copy Function with name parquet does not exist" if the
	// extension isn't loaded, which is the right user signal.
	CatalogEntryRetriever entry_retriever {context};
	auto &system_catalog = Catalog::GetSystemCatalog(context);
	auto entry =
	    system_catalog.GetEntry(entry_retriever, DEFAULT_SCHEMA, {CatalogType::COPY_FUNCTION_ENTRY, format_name},
	                            OnEntryNotFound::THROW_EXCEPTION);
	auto &copy_function_entry = entry->Cast<CopyFunctionCatalogEntry>();
	CopyFunction function = copy_function_entry.function;
	if (!function.copy_to_bind) {
		throw NotImplementedException("COPY function '%s' does not support writing", format_name);
	}

	// Collect column names and types from the table definition (physical columns only).
	vector<string> column_names;
	vector<LogicalType> column_types;
	auto &columns = hms_table.GetColumns();
	for (auto &col : columns.Physical()) {
		column_names.push_back(col.Name());
		column_types.push_back(col.Type());
	}

	auto copy_info = make_uniq<CopyInfo>();
	copy_info->format = format_name;
	copy_info->file_path = hms_table.table_data->storage_location;
	if (format_result.format == hms::TableFormat::CSV) {
		// Hive's LazySimpleSerDe stores no header line; align the DuckDB CSV writer accordingly.
		copy_info->options["header"] = {Value::BOOLEAN(false)};
		// Match the SerDe delimiter we record on CREATE TABLE (',').
		auto delim_it = hms_table.table_data->serde_parameters.find(hms::serde_param::FIELD_DELIM);
		string delim = delim_it != hms_table.table_data->serde_parameters.end() ? delim_it->second : ",";
		copy_info->options["delim"] = {Value(delim)};
	}
	CopyFunctionBindInput bind_input(*copy_info);
	bind_input.file_extension = function.extension;
	auto bind_data = function.copy_to_bind(context, bind_input, column_names, column_types);

	// Cloud schemes (s3a, oss, cos, cosn) are rewritten to s3:// so DuckDB's httpfs can write.
	string write_dir = hms::PathUtils::RewriteS3CompatibleScheme(hms_table.table_data->storage_location);

	// UUID-suffixed filename — avoids collisions across concurrent INSERTs into the same table.
	string filename = "data_" + UUID::ToString(UUID::GenerateRandomUUID()) + "." + function.extension;
	string file_path = JoinDirAndFile(write_dir, filename);

	auto &copy_op =
	    planner.Make<PhysicalCopyToFile>(output_types, function, std::move(bind_data), estimated_cardinality);
	auto &cast_copy = copy_op.Cast<PhysicalCopyToFile>();
	cast_copy.file_path = file_path;
	cast_copy.use_tmp_file = false; // remote object stores: tmp_file + rename is slow/unsafe
	cast_copy.overwrite_mode = CopyOverwriteMode::COPY_ERROR_ON_CONFLICT;
	cast_copy.file_extension = function.extension;
	cast_copy.per_thread_output = false;
	cast_copy.rotate = false;
	cast_copy.return_type = CopyFunctionReturnType::CHANGED_ROWS;
	cast_copy.partition_output = false;
	cast_copy.write_partition_columns = false;
	cast_copy.names = column_names;
	cast_copy.expected_types = column_types;
	cast_copy.parallel = false; // single-threaded write; safer default for one-file-per-INSERT
	cast_copy.write_empty_file = false;
	cast_copy.hive_file_pattern = false;
	cast_copy.children.push_back(child_plan);
	return copy_op;
}

PhysicalOperator &HMSCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	if (!plan) {
		throw NotImplementedException("INSERT without a source plan is not supported on HMS tables");
	}
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING is not supported for INSERT on HMS tables");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("ON CONFLICT is not supported for INSERT on HMS tables");
	}

	auto &hms_table = op.table.Cast<HMSTableEntry>();

	// Partial-column INSERT needs a defaults projection so the child plan's output
	// width matches the table's physical column count.
	optional_ptr<PhysicalOperator> insert_plan = plan;
	if (!op.column_index_map.empty()) {
		insert_plan = planner.ResolveDefaultsProjection(op, *insert_plan);
	}

	return PlanHMSWrite(context, planner, hms_table, *insert_plan, op.types, op.estimated_cardinality);
}

PhysicalOperator &HMSCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	// Pre-validate the requested format BEFORE registering the table in HMS. Without this guard,
	// CTAS to an unsupported format (avro/delta/iceberg/orc) creates the metastore entry, then
	// PlanHMSWrite throws NotImplementedException — leaving an orphan entry that the user has to
	// DROP manually. Force WITH-clause options to be folded into tags first; SupportsCreateTable
	// is idempotent (it clears base.options after the first pass), so calling it here and again
	// from inside Catalog::CreateTable below is harmless.
	auto support_err = SupportsCreateTable(*op.info);
	if (support_err.HasError()) {
		support_err.Throw();
	}
	auto &base = op.info->Base();
	string fmt;
	auto fmt_it = base.tags.find("format");
	if (fmt_it != base.tags.end()) {
		fmt = fmt_it->second;
	} else {
		auto prov_it = base.tags.find("provider");
		if (prov_it != base.tags.end()) {
			fmt = prov_it->second;
		}
	}
	fmt = StringUtil::Lower(fmt);
	StringUtil::Trim(fmt);
	if (!fmt.empty() && fmt != hms::format::PARQUET && fmt != hms::format::CSV) {
		throw NotImplementedException(
		    "CREATE TABLE AS is not supported for format '%s' on HMS tables (only Parquet and CSV are writable).", fmt);
	}

	// Route through Catalog::CreateTable so the schema-level CreateTable path runs (consistent
	// with non-CTAS CREATE TABLE). If the subsequent write fails after this point we leave an
	// empty entry behind — that case is exercised by test/sql/parquet/ctas_orphan_metadata.test.
	auto transaction = GetCatalogTransaction(context);
	auto created_entry = CreateTable(transaction, op.schema, *op.info);
	if (!created_entry) {
		throw IOException("CREATE TABLE AS: failed to register table '%s' in the Hive Metastore", base.table);
	}
	auto &hms_table = created_entry->Cast<HMSTableEntry>();
	return PlanHMSWrite(context, planner, hms_table, plan, op.types, op.estimated_cardinality);
}

PhysicalOperator &HMSCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("HMSCatalog PlanDelete");
}

PhysicalOperator &HMSCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	throw NotImplementedException("HMSCatalog PlanDelete");
}

PhysicalOperator &HMSCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("HMSCatalog PlanUpdate");
}

unique_ptr<LogicalOperator> HMSCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                        unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("HMSCatalog BindCreateIndex");
}

} // namespace duckdb

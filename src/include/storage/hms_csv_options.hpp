//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_csv_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class ClientContext;
struct HMSAPITable;

//! Adds the read_csv options that parse the files of a CSV or text table, from what the metastore records about how
//! they were written: the dialect of a Spark CSV table, or Hive's LazySimpleSerDe format. The files of a partitioned
//! table are never sniffed: sniffing reads files while binding, before the filters that select the partitions are
//! known.
void AddCSVReadOptions(ClientContext &context, const HMSAPITable &table, bool has_spark_schema, bool partitioned,
                       case_insensitive_map_t<Value> &options);

//! A Java DateTimeFormatter pattern (Spark's dateFormat and timestampFormat) as a strftime format; false if some part
//! of it has none
bool TranslateJavaDateFormat(const string &pattern, string &result);

} // namespace duckdb

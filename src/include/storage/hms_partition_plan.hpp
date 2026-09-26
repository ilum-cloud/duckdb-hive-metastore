//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_partition_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Where the values of the partition columns come from
enum class HMSPartitionSource : uint8_t {
	//! The partitions registered in the metastore: values and location come from the partition record
	HMS,
	//! The file paths: values are parsed from their key=value segments, as DuckDB does natively
	PATH
};

//! One partition to scan
struct HMSScanPartition {
	//! Display name ("k=v/k=v"), for warnings and errors
	string name;
	//! The partition location, normalized the same way a table location is (scheme rewrites and so on)
	string location;
	//! The glob matching the data files of this partition
	string scan_location;
	//! A permissive glob, used when the first one matches nothing: Hive writes files without an extension
	string fallback_scan_location;
	//! One value per partition column, already cast to the metastore type
	vector<Value> values;
};

//! How a scan of a partitioned table locates its files and fills its partition columns. Built once per table entry
//! and reused until the catalog's metadata cache TTL expires.
struct HMSPartitionPlan {
	HMSPartitionSource source = HMSPartitionSource::PATH;
	//! Partition column names, in the order the metastore declares them
	vector<string> names;
	//! Partition column types, in the same order
	vector<LogicalType> types;
	//! The partitions to scan; only set when source is HMS
	vector<HMSScanPartition> partitions;
	//! Set when a partition location needs the S3 endpoint configured (an http:// location)
	bool needs_s3_config = false;
	string s3_endpoint;
};

} // namespace duckdb

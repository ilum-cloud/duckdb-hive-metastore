//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_partition_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

//! How the files of a partitioned table are located (ATTACH option PARTITION_MODE)
enum class HMSPartitionMode : uint8_t {
	//! Use the partitions registered in the metastore; fall back to the table location when there are none
	AUTO,
	//! Always use the partitions registered in the metastore; error when a partitioned table has none
	HMS,
	//! Never read the partition list; glob the table location and read partition values from key=value paths
	PATH
};

//! Which filters on partition columns are sent to the metastore (ATTACH option PARTITION_FILTER_PUSHDOWN)
enum class HMSPartitionFilterPushdown : uint8_t {
	//! Only filters the metastore evaluates exactly as DuckDB does, or as a superset of it
	EXACT,
	//! Also ranges and inequalities on string keys, which the metastore compares with its database's collation
	ALL,
	//! None: the partitions are selected by evaluating the filters on their names
	OFF
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_format_detector.hpp
//
// Centralized format detection for Hive Metastore tables
//
//===----------------------------------------------------------------------===//

#pragma once

#include "hms_api.hpp"
#include "hms_constants.hpp"

namespace duckdb {
namespace hms {

// Enum for detected table formats
enum class TableFormat { PARQUET, DELTA, ICEBERG, AVRO, CSV, UNKNOWN };

// Result of format detection
struct FormatDetectionResult {
	TableFormat format;
	bool is_partitioned;

	FormatDetectionResult() : format(TableFormat::UNKNOWN), is_partitioned(false) {
	}
	explicit FormatDetectionResult(TableFormat format_p, bool is_partitioned_p = false)
	    : format(format_p), is_partitioned(is_partitioned_p) {
	}

	bool IsDelta() const {
		return format == TableFormat::DELTA;
	}
	bool IsIceberg() const {
		return format == TableFormat::ICEBERG;
	}
	bool IsParquet() const {
		return format == TableFormat::PARQUET;
	}
	bool IsAvro() const {
		return format == TableFormat::AVRO;
	}
	bool IsCSV() const {
		return format == TableFormat::CSV;
	}
	bool IsUnknown() const {
		return format == TableFormat::UNKNOWN;
	}
};

// Detects the format of a Hive Metastore table
class FormatDetector {
public:
	// Detect format from table metadata
	static FormatDetectionResult Detect(const HMSAPITable &table_data);

	// Get the scan function name for a given format
	static const char *GetScanFunctionName(TableFormat format);

	// Check if format requires auto-loading an extension
	static bool RequiresExtension(TableFormat format);

	// Get the required extension name for a format
	static const char *GetExtensionName(TableFormat format);

private:
	// Check format from parameters (provider, table_type)
	static bool CheckFormatFromParameters(const HMSAPITable &table_data, bool &is_delta, bool &is_iceberg);

	// Check format from input format and serialization lib
	static TableFormat CheckFormatFromStorageDescriptor(const HMSAPITable &table_data);
};

} // namespace hms
} // namespace duckdb

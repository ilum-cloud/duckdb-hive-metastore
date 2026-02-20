#include "hms_format_detector.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace hms {

bool FormatDetector::CheckFormatFromParameters(const HMSAPITable &table_data, bool &is_delta, bool &is_iceberg) {
	// Reset flags
	is_delta = false;
	is_iceberg = false;

	// Check spark.sql.sources.provider = delta
	auto provider_it = table_data.parameters.find(spark_param::PROVIDER);
	if (provider_it != table_data.parameters.end() && StringUtil::CIEquals(provider_it->second, format::DELTA)) {
		is_delta = true;
		return true;
	}

	// Check table_type parameter
	auto table_type_it = table_data.parameters.find(table_type::TABLE_TYPE);
	if (table_type_it != table_data.parameters.end()) {
		if (StringUtil::CIEquals(table_type_it->second, table_type::ICEBERG)) {
			is_iceberg = true;
			return true;
		} else if (StringUtil::CIEquals(table_type_it->second, table_type::DELTA)) {
			is_delta = true;
			return true;
		}
	}

	return false;
}

TableFormat FormatDetector::CheckFormatFromStorageDescriptor(const HMSAPITable &table_data) {
	// Check input format and serialization lib for format indicators
	if (StringUtil::Contains(table_data.input_format, storage_format::ICEBERG_INPUT_FORMAT)) {
		return TableFormat::ICEBERG;
	}
	if (StringUtil::Contains(table_data.input_format, "DELTA") ||
	    StringUtil::Contains(table_data.serialization_lib, storage_format::DELTA_SERDE)) {
		return TableFormat::DELTA;
	}
	if (StringUtil::Contains(table_data.input_format, storage_format::PARQUET_INPUT) ||
	    StringUtil::Contains(table_data.serialization_lib, storage_format::PARQUET_SERDE)) {
		return TableFormat::PARQUET;
	}
	if (StringUtil::Contains(table_data.input_format, storage_format::AVRO_INPUT) ||
	    StringUtil::Contains(table_data.serialization_lib, storage_format::AVRO_SERDE)) {
		return TableFormat::AVRO;
	}
	if (StringUtil::Contains(table_data.input_format, storage_format::TEXT_INPUT) ||
	    StringUtil::Contains(table_data.serialization_lib, storage_format::LAZY_SIMPLE_SERDE)) {
		return TableFormat::CSV;
	}

	return TableFormat::UNKNOWN;
}

FormatDetectionResult FormatDetector::Detect(const HMSAPITable &table_data) {
	bool is_delta = false;
	bool is_iceberg = false;

	// First check parameters for delta/iceberg
	CheckFormatFromParameters(table_data, is_delta, is_iceberg);

	// If not found in parameters (or not delta/iceberg), check storage descriptor
	TableFormat format = TableFormat::UNKNOWN;
	if (is_delta) {
		format = TableFormat::DELTA;
	} else if (is_iceberg) {
		format = TableFormat::ICEBERG;
	} else {
		// Only check storage descriptor if we didn't find delta/iceberg in parameters
		format = CheckFormatFromStorageDescriptor(table_data);
	}

	bool is_partitioned = !table_data.partition_keys.empty();
	return FormatDetectionResult(format, is_partitioned);
}

const char *FormatDetector::GetScanFunctionName(TableFormat format) {
	switch (format) {
	case TableFormat::DELTA:
		return scan_function::DELTA_SCAN;
	case TableFormat::ICEBERG:
		return scan_function::ICEBERG_SCAN;
	case TableFormat::PARQUET:
		return scan_function::PARQUET_SCAN;
	case TableFormat::AVRO:
		return scan_function::READ_AVRO;
	case TableFormat::CSV:
		return scan_function::READ_CSV;
	default:
		return nullptr;
	}
}

bool FormatDetector::RequiresExtension(TableFormat format) {
	return format == TableFormat::DELTA || format == TableFormat::ICEBERG;
}

const char *FormatDetector::GetExtensionName(TableFormat format) {
	switch (format) {
	case TableFormat::DELTA:
		return format::DELTA;
	case TableFormat::ICEBERG:
		return format::ICEBERG;
	default:
		return nullptr;
	}
}

} // namespace hms
} // namespace duckdb

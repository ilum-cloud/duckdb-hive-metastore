//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_path_utils.hpp
//
// Path normalization utilities for Hive Metastore table locations
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "hms_api.hpp"
#include "hms_constants.hpp"
#include "hms_format_detector.hpp"

namespace duckdb {
namespace hms {

// Result of path normalization
struct NormalizedPathResult {
	string scan_path;
	bool needs_s3_config;
	string s3_endpoint;

	NormalizedPathResult() : needs_s3_config(false) {
	}
};

// Path normalization utilities
class PathUtils {
public:
	// Normalize a storage path for scanning
	// Handles:
	// - Stripping __PLACEHOLDER__ suffix
	// - Converting s3a:// to s3://
	// - Converting http:// to s3:// with endpoint config
	// - Adding trailing slashes for Delta tables
	// - Removing trailing slashes for Iceberg tables
	static NormalizedPathResult NormalizeScanPath(const string &storage_path, const HMSAPITable &table_data,
	                                              const FormatDetectionResult &format);

	// Build a glob pattern for directory-based scans
	// e.g., /path/to/table -> /path/to/table/**/*.parquet
	static string BuildGlobPattern(const string &path, const FormatDetectionResult &format, bool is_partitioned);

	// Check if a path looks like a file (has extension)
	static bool HasFileExtension(const string &path);

	// Strip the __PLACEHOLDER__ suffix from a path
	static string StripPlaceholder(const string &path);

	// Convert s3a:// to s3://
	static string ConvertS3AToS3(const string &path);

	// Ensure proper trailing slash for Delta tables
	static string EnsureTrailingSlash(const string &path);

	// Remove trailing slash for Iceberg tables
	static string RemoveTrailingSlash(const string &path);

private:
	// Try to get location from table parameters
	static string GetLocationFromParams(const map<string, string> &parameters);
};

} // namespace hms
} // namespace duckdb

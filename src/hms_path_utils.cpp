#include "hms_path_utils.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace hms {

string PathUtils::StripPlaceholder(const string &path) {
	// Check for "-__PLACEHOLDER__" suffix
	if (StringUtil::Contains(path, path::PLACEHOLDER_SUFFIX)) {
		auto placeholder_pos = path.find(path::PLACEHOLDER_SUFFIX);
		if (placeholder_pos != string::npos) {
			return path.substr(0, placeholder_pos);
		}
	}
	return path;
}

string PathUtils::ConvertS3AToS3(const string &path) {
	if (StringUtil::StartsWith(path, url::S3A)) {
		return string(url::S3) + path.substr(6);
	}
	return path;
}

string PathUtils::EnsureTrailingSlash(const string &path) {
	if (!StringUtil::EndsWith(path, "/")) {
		return path + "/";
	}
	return path;
}

string PathUtils::RemoveTrailingSlash(const string &path) {
	if (StringUtil::EndsWith(path, "/")) {
		return path.substr(0, path.length() - 1);
	}
	return path;
}

string PathUtils::GetLocationFromParams(const map<string, string> &parameters) {
	auto location_it = parameters.find("location");
	if (location_it != parameters.end()) {
		return location_it->second;
	}
	auto path_it = parameters.find("path");
	if (path_it != parameters.end()) {
		return path_it->second;
	}
	return string();
}

NormalizedPathResult PathUtils::NormalizeScanPath(const string &storage_path, const HMSAPITable &table_data,
                                                  const FormatDetectionResult &format) {
	NormalizedPathResult result;
	string scan_path = storage_path;

	// Strip __PLACEHOLDER__ if present (for any table type, not just Delta)
	// This handles Spark's placeholder suffix that appears in some table paths
	if (StringUtil::Contains(scan_path, path::PLACEHOLDER)) {
		auto placeholder_pos = scan_path.find(path::PLACEHOLDER_SUFFIX);
		if (placeholder_pos != string::npos) {
			scan_path = scan_path.substr(0, placeholder_pos);
		}
	}

	// For Delta tables, also check for location/path parameters
	if (format.IsDelta()) {
		string param_path = GetLocationFromParams(table_data.parameters);
		if (!param_path.empty()) {
			scan_path = param_path;
		}
	}

	// Handle HTTP to S3 conversion (for MinIO compatibility)
	if (StringUtil::StartsWith(scan_path, url::HTTP)) {
		constexpr size_t HTTP_PREFIX_LEN = 7;
		auto no_scheme = scan_path.substr(HTTP_PREFIX_LEN);
		auto first_slash = no_scheme.find('/');

		if (first_slash != string::npos) {
			string host_port = no_scheme.substr(0, first_slash);
			string path = no_scheme.substr(first_slash + 1);

			// Auto-configure if it looks like a private S3 endpoint (has port)
			// or is not the standard s3.amazonaws.com domain
			bool has_port = host_port.find(':') != string::npos;
			bool is_standard_aws = host_port.find("s3.amazonaws.com") != string::npos;

			if (has_port || !is_standard_aws) {
				result.needs_s3_config = true;
				result.s3_endpoint = host_port;
				scan_path = string(url::S3) + path;
			}
		}
	}
	// Convert s3a:// to s3://
	else if (StringUtil::StartsWith(scan_path, url::S3A)) {
		scan_path = ConvertS3AToS3(scan_path);
	}

	// Format-specific path adjustments
	if (format.IsDelta()) {
		scan_path = EnsureTrailingSlash(scan_path);
	} else if (format.IsIceberg()) {
		scan_path = RemoveTrailingSlash(scan_path);
	}

	result.scan_path = scan_path;
	return result;
}

bool PathUtils::HasFileExtension(const string &path) {
	auto last_dot = path.find_last_of('.');
	auto last_slash = path.find_last_of('/');
	return last_dot != string::npos && (last_slash == string::npos || last_dot > last_slash);
}

string PathUtils::BuildGlobPattern(const string &path, const FormatDetectionResult &format, bool is_partitioned) {
	// Don't add glob if path already has a file extension
	if (HasFileExtension(path)) {
		return path;
	}

	// Delta and Iceberg don't need globs
	if (format.IsDelta() || format.IsIceberg()) {
		return path;
	}

	string glob;
	if (format.IsParquet()) {
		// For partitioned tables, use recursive glob
		glob = is_partitioned ? "/**/*.parquet" : "/*.parquet";
	} else if (format.IsAvro()) {
		// For partitioned tables, use recursive glob
		glob = is_partitioned ? "/**/*.avro" : "/*.avro";
	} else {
		// For CSV/Text, use * to match any file
		// Exclude files starting with _ (like _SUCCESS)
		glob = is_partitioned ? "/**/[!_]*" : "/[!_]*";
	}

	if (StringUtil::EndsWith(path, "/")) {
		return path + glob.substr(1);
	}
	return path + glob;
}

} // namespace hms
} // namespace duckdb

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class HiveMetastoreExtension : public Extension {
public:
	void Load(ExtensionLoader &load) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

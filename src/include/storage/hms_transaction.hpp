//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class HMSCatalog;
class HMSCatalogSet;
class HMSSchemaEntry;
class HMSTableEntry;

enum class HMSTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class HMSTransaction : public Transaction {
public:
	HMSTransaction(HMSCatalog &hms_catalog, TransactionManager &manager, ClientContext &context);
	~HMSTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	//	UCConnection &GetConnection();
	//	unique_ptr<UCResult> Query(const string &query);
	static HMSTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

	//! Metadata snapshot of the transaction: once a name has been resolved (or found missing) in a catalog set, the
	//! rest of the transaction sees the same result
	bool TryGetEntry(const HMSCatalogSet &set, const string &name, optional_ptr<CatalogEntry> &result);
	//! Records a resolved name (nullptr: it does not exist) and returns what was recorded first
	optional_ptr<CatalogEntry> RecordEntry(const HMSCatalogSet &set, const string &name,
	                                       optional_ptr<CatalogEntry> entry);
	bool TryGetScan(const HMSCatalogSet &set, vector<reference<CatalogEntry>> &result);
	//! Records a scan, keeping the versions of entries this transaction already resolved; returns the recorded scan
	vector<reference<CatalogEntry>> RecordScan(const HMSCatalogSet &set, const vector<reference<CatalogEntry>> &scan);
	//! Records an entry created (or dropped, if nullptr) by this transaction
	void OverrideEntry(const HMSCatalogSet &set, const string &name, optional_ptr<CatalogEntry> entry);
	void ClearSnapshot();

private:
	struct SetSnapshot {
		case_insensitive_map_t<optional_ptr<CatalogEntry>> entries;
		bool scanned = false;
		vector<reference<CatalogEntry>> scan;
	};

	//	UCConnection connection;
	HMSTransactionState transaction_state;
	AccessMode access_mode;
	mutex snapshot_lock;
	unordered_map<const HMSCatalogSet *, SetSnapshot> snapshots;
};

} // namespace duckdb

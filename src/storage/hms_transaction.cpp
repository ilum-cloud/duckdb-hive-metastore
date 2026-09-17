#include "storage/hms_transaction.hpp"
#include "storage/hms_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

HMSTransaction::HMSTransaction(HMSCatalog &hms_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), access_mode(hms_catalog.access_mode) {
	//	connection = UCConnection::Open(hms_catalog.path);
}

HMSTransaction::~HMSTransaction() = default;

void HMSTransaction::Start() {
	transaction_state = HMSTransactionState::TRANSACTION_NOT_YET_STARTED;
}
void HMSTransaction::Commit() {
	if (transaction_state == HMSTransactionState::TRANSACTION_STARTED) {
		transaction_state = HMSTransactionState::TRANSACTION_FINISHED;
		//		connection.Execute("COMMIT");
	}
}
void HMSTransaction::Rollback() {
	if (transaction_state == HMSTransactionState::TRANSACTION_STARTED) {
		transaction_state = HMSTransactionState::TRANSACTION_FINISHED;
		//		connection.Execute("ROLLBACK");
	}
}

// UCConnection &HMSTransaction::GetConnection() {
//	if (transaction_state == HMSTransactionState::TRANSACTION_NOT_YET_STARTED) {
//		transaction_state = HMSTransactionState::TRANSACTION_STARTED;
//		string query = "START TRANSACTION";
//		if (access_mode == AccessMode::READ_ONLY) {
//			query += " READ ONLY";
//		}
//		conne/**/ction.Execute(query);
//	}
//	return connection;
//}

// unique_ptr<UCResult> HMSTransaction::Query(const string &query) {
//	if (transaction_state == HMSTransactionState::TRANSACTION_NOT_YET_STARTED) {
//		transaction_state = HMSTransactionState::TRANSACTION_STARTED;
//		string transaction_start = "START TRANSACTION";
//		if (access_mode == AccessMode::READ_ONLY) {
//			transaction_start += " READ ONLY";
//		}
//		connection.Query(transaction_start);
//		return connection.Query(query);
//	}
//	return connection.Query(query);
//}

HMSTransaction &HMSTransaction::Get(ClientContext &context, Catalog &catalog) {
	// Transaction::Get returns a reference and will throw if no transaction exists
	// We just need to cast it to the correct type
	return Transaction::Get(context, catalog).Cast<HMSTransaction>();
}

bool HMSTransaction::TryGetEntry(const HMSCatalogSet &set, const string &name, optional_ptr<CatalogEntry> &result) {
	lock_guard<mutex> guard(snapshot_lock);
	auto snapshot = snapshots.find(&set);
	if (snapshot == snapshots.end()) {
		return false;
	}
	auto entry = snapshot->second.entries.find(name);
	if (entry == snapshot->second.entries.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

optional_ptr<CatalogEntry> HMSTransaction::RecordEntry(const HMSCatalogSet &set, const string &name,
                                                       optional_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(snapshot_lock);
	auto &snapshot = snapshots[&set];
	auto recorded = snapshot.entries.find(name);
	if (recorded != snapshot.entries.end()) {
		return recorded->second;
	}
	snapshot.entries[name] = entry;
	return entry;
}

bool HMSTransaction::TryGetScan(const HMSCatalogSet &set, vector<reference<CatalogEntry>> &result) {
	lock_guard<mutex> guard(snapshot_lock);
	auto snapshot = snapshots.find(&set);
	if (snapshot == snapshots.end() || !snapshot->second.scanned) {
		return false;
	}
	result = snapshot->second.scan;
	return true;
}

vector<reference<CatalogEntry>> HMSTransaction::RecordScan(const HMSCatalogSet &set,
                                                           const vector<reference<CatalogEntry>> &scan) {
	lock_guard<mutex> guard(snapshot_lock);
	auto &snapshot = snapshots[&set];
	if (snapshot.scanned) {
		return snapshot.scan;
	}
	vector<reference<CatalogEntry>> result;
	case_insensitive_set_t scanned_names;
	for (auto &entry : scan) {
		auto &name = entry.get().name;
		scanned_names.insert(name);
		auto resolved = snapshot.entries.find(name);
		if (resolved == snapshot.entries.end()) {
			snapshot.entries[name] = &entry.get();
			result.push_back(entry);
		} else if (resolved->second) {
			// Keep the version this transaction already resolved
			result.push_back(*resolved->second);
		}
		// Otherwise the transaction already found the entry missing: leave it out
	}
	for (auto &resolved : snapshot.entries) {
		if (resolved.second && scanned_names.find(resolved.first) == scanned_names.end()) {
			// Resolved or created earlier in this transaction, but not (or no longer) listed
			result.push_back(*resolved.second);
		}
	}
	snapshot.scanned = true;
	snapshot.scan = result;
	return result;
}

void HMSTransaction::OverrideEntry(const HMSCatalogSet &set, const string &name, optional_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(snapshot_lock);
	auto &snapshot = snapshots[&set];
	snapshot.entries[name] = entry;
	snapshot.scanned = false;
	snapshot.scan.clear();
}

void HMSTransaction::ClearSnapshot() {
	lock_guard<mutex> guard(snapshot_lock);
	snapshots.clear();
}

} // namespace duckdb

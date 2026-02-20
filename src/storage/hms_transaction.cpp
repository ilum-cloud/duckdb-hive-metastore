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

} // namespace duckdb

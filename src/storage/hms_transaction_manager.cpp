#include "storage/hms_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

HMSTransactionManager::HMSTransactionManager(AttachedDatabase &db_p, HMSCatalog &hms_catalog)
    : TransactionManager(db_p), hms_catalog(hms_catalog) {
}

Transaction &HMSTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<HMSTransaction>(hms_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData HMSTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &hms_transaction = transaction.Cast<HMSTransaction>();
	hms_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void HMSTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &hms_transaction = transaction.Cast<HMSTransaction>();
	hms_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void HMSTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// auto &transaction = HMSTransaction::Get(context, db.GetCatalog());
	//	auto &db = transaction.GetConnection();
	//	db.Execute("CHECKPOINT");
}

} // namespace duckdb

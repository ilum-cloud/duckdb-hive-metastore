//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/hms_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/hms_catalog.hpp"
#include "storage/hms_transaction.hpp"

namespace duckdb {

class HMSTransactionManager : public TransactionManager {
public:
	HMSTransactionManager(AttachedDatabase &db_p, HMSCatalog &hms_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	HMSCatalog &hms_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<HMSTransaction>> transactions;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      schema_(Schema(std::vector<Column>())),
      table_iter_(TableIterator(nullptr, RID(), nullptr)),
      end_(TableIterator(nullptr, RID(), nullptr)) {
  // std::ifstream file("/autograder/bustub/test/execution/grading_sequential_scan_executor_test.cpp");
  // std::string str;
  // while (file.good()) {
  //   std::getline(file, str);
  //   std::cout << str << std::endl;
  // }
}

void SeqScanExecutor::Init() {
  TableInfo *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // table schema
  schema_ = table_info->schema_;
  // iterator
  table_iter_ = table_info->table_->Begin(exec_ctx_->GetTransaction());
  end_ = table_info->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();

  // expression
  const AbstractExpression *predicate = plan_->GetPredicate();
  while (table_iter_ != end_) {
    // lock
    if (!lock_manager->LockShared(txn, table_iter_->GetRid())) {
      // throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(),
      // AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    const Tuple tmp = *table_iter_;
    table_iter_++;
    // unlock for read commit
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (!lock_manager->Unlock(txn, tmp.GetRid())) {
        // throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(),
        // AbortReason::UNLOCK_ON_SHRINKING);
        return false;
      }
    }

    // caution!!! predicate is nullptr!!!
    if (predicate == nullptr || predicate->Evaluate(&tmp, &schema_).GetAs<bool>()) {
      // get Rid
      *rid = tmp.GetRid();

      // for the output tuple
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->Evaluate(&tmp, &schema_));
      }

      // new tuples
      *tuple = Tuple(values, GetOutputSchema());
      // if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //   if (!exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(),table_iter_->GetRid())) {
      //     exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
      //     // throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(),
      //     // AbortReason::UNLOCK_ON_SHRINKING);
      //     return false;
      //   }
      // }
      return true;
    }
    // if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    //   if (!exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(),table_iter_->GetRid())) {
    //     exec_ctx_->GetTransactionManager()->Abort(exec_ctx_->GetTransaction());
    //     // throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(),
    //     // AbortReason::UNLOCK_ON_SHRINKING);
    //     return false;
    //   }
    // }
  }
  // // final success
  // txn_mgr->Commit(txn);
  return false;
}

}  // namespace bustub

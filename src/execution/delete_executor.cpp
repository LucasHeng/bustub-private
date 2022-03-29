//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() {
  // init the indexes of target table
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // init the child executor
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();

  // first,get the tuple from child executor
  Tuple del_tuple;
  if (!child_executor_->Next(&del_tuple, rid)) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "child executor error");
    return false;
  }

  // Acquire an exclusive lock, upgrading from a shared lock if necessary.
  if (txn->IsSharedLocked(*rid)) {
    if (!lock_manager->LockUpgrade(txn, *rid)) {
      return false;
    }
  } else if (!txn->IsExclusiveLocked(*rid) && !lock_manager->LockExclusive(txn, *rid)) {
    return false;
  }

  // delete it from table
  if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  // delete it from indexes
  for (auto &indexinfo : table_indexes_) {
    // first,get the key tuple
    const std::vector<uint32_t> &key_attrs = indexinfo->index_->GetKeyAttrs();
    Tuple key_tuple = del_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, key_attrs);
    indexinfo->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub

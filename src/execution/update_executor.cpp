//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  // init the indexes of target table
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // init the child executor
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  Transaction *txn = exec_ctx_->GetTransaction();

  // first,get the tuple from child executor
  Tuple old_tuple;
  if (!child_executor_->Next(&old_tuple, rid)) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "child executor error");
  }

  // Acquire an exclusive lock, upgrading from a shared lock if necessary.
  if (txn->IsSharedLocked(*rid)) {
    if (!lock_manager->LockUpgrade(txn, *rid)) {
      return false;
    }
  } else if (!txn->IsExclusiveLocked(*rid) && !lock_manager->LockExclusive(txn, *rid)) {
    return false;
  }

  // then,get the new tuple by generateupdatetuple
  Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
  // update the tabale heap,new tuple,old rid,txn
  // LOG_DEBUG("%d\n",rid->GetSlotNum());
  if (!table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  // update all indexes in the table
  const auto &update_attrs = plan_->GetUpdateAttr();
  for (auto &indexinfo : table_indexes_) {
    //  first,judge whether the index is changed  or not
    const std::vector<uint32_t> &key_attrs = indexinfo->index_->GetKeyAttrs();
    bool is_changed = false;
    for (auto &idx : key_attrs) {
      if (update_attrs.find(idx) != update_attrs.cend()) {
        is_changed = true;
        break;
      }
    }
    // if the index should be updated
    if (is_changed) {
      Tuple old_key = old_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, key_attrs);
      Tuple new_key = new_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, key_attrs);
      indexinfo->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
      indexinfo->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
    }
  }
  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub

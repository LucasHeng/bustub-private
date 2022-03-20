//===----------------------------------------------------------------------===//
//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      target_table_(nullptr),
      row_idx_(0),
      row_size_(0),
      child_executor_(child_executor.release()) {}

void InsertExecutor::Init() {
  // get the target table information
  target_table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  // get the indexes of target table
  table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(target_table_->name_);
  if (plan_->IsRawInsert()) {
    // raw insert
    // get the row_size
    row_size_ = plan_->RawValues().size();
  } else {
    // insert the value from child node
    // init child executor
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple tmp_tuple;
  if (plan_->IsRawInsert()) {
    // raw insert
    // table insert,first construct the tuple
    if (row_idx_ >= row_size_) {
      return false;
    }
    std::vector<Value> value(plan_->RawValuesAt(row_idx_++));
    tmp_tuple = Tuple(value, &(target_table_->schema_));
  } else {
    if (!child_executor_->Next(&tmp_tuple, rid)) {
      return false;
    }
  }
  // then insert into table heap
  assert(target_table_->table_.get()->InsertTuple(tmp_tuple, rid, exec_ctx_->GetTransaction()));

  // table indexes insert,first loop all indexes
  for (auto &indexinfo : table_indexes_) {
    // first construct the tuple key of index
    Index *index = indexinfo->index_.get();
    Tuple key = tmp_tuple.KeyFromTuple(target_table_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
    // insert tuple key
    index->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub

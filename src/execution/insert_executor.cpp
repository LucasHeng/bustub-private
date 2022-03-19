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
      table_indexes(std::vector<IndexInfo *>()),
      row_idx(0),
      row_size(0),
      child_executor(nullptr) {}

void InsertExecutor::Init() {
  // get the target table information
  target_table_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  // get the indexes of target table
  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(target_table_->name_);  
  if (plan_->IsRawInsert()) {
    // raw insert
    // get the row_size
    row_size = plan_->RawValues().size();
  } else {
    // insert the value from child node
    // init child executor
    auto child_node = plan_->GetChildPlan();
    child_executor.reset(ExecutorFactory::CreateExecutor(exec_ctx_, child_node).release());
    child_executor->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    Tuple tuple_;
  if (plan_->IsRawInsert()) {
    // raw insert
    // table insert,first construct the tuple
    if(row_idx >= row_size) {
        return false;
    }
    std::vector<Value> value(plan_->RawValuesAt(row_idx++));
    tuple_ = Tuple(value, &(target_table_->schema_));
  } else {
      if(!child_executor->Next(&tuple_, rid)) {
          return false;
      }
  }
  // then insert into table heap
  assert(target_table_->table_.get()->InsertTuple(tuple_, rid, exec_ctx_->GetTransaction()));

  // table indexes insert,first loop all indexes
  for (auto &indexinfo : table_indexes) {
    // first construct the tuple key of index
    Index *index = indexinfo->index_.get();
    Tuple key = tuple_.KeyFromTuple(target_table_->schema_, *index->GetKeySchema(), index->GetKeyAttrs());  
    // insert tuple key
    index->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
  }
  return true;
}
}  // namespace bustub

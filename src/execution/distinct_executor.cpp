//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor.release()) {}

void DistinctExecutor::Init() {
  Tuple tuple;
  RID rid;
  // don't forget to init the child executor
  child_executor_->Init();
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    for (uint32_t col_idx = 0; col_idx < GetOutputSchema()->GetColumnCount(); col_idx++) {
      values.emplace_back(tuple.GetValue(GetOutputSchema(), col_idx));
    }
    dist_set_.emplace(DistKey{values});
  }
  // LOG_DEBUG("%ld\n", dist_set_.size());
  ds_iter_ = dist_set_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (ds_iter_ != dist_set_.end()) {
    *tuple = Tuple(ds_iter_->values_, GetOutputSchema());
    ds_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub

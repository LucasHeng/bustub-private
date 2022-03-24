//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(left_child.release()), right_child_(right_child.release()) {}

void HashJoinExecutor::Init() {
  // Initialize the executors
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  // construct the hash table
  while (left_child_->Next(&tuple, &rid)) {
    Value value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
    JoinKey key = {value};
    join_map_[key].emplace_back(tuple);
  }
  // construct the join result tuples
  while (right_child_->Next(&tuple, &rid)) {
    Value value = plan_->RightJoinKeyExpression()->Evaluate(&tuple, right_child_->GetOutputSchema());
    JoinKey key = {value};
    if (join_map_.count(key) != 0) {
      for (auto &l_tuple : join_map_[key]) {
        // form new schema and new join tuple
        std::vector<Column> columns;
        std::vector<Value> values;
        for (uint32_t j = 0; j < left_child_->GetOutputSchema()->GetColumnCount(); j++) {
          columns.emplace_back(left_child_->GetOutputSchema()->GetColumn(j));
          values.emplace_back(l_tuple.GetValue(left_child_->GetOutputSchema(), j));
        }
        for (uint32_t j = 0; j < right_child_->GetOutputSchema()->GetColumnCount(); j++) {
          columns.emplace_back(right_child_->GetOutputSchema()->GetColumn(j));
          values.emplace_back(tuple.GetValue(right_child_->GetOutputSchema(), j));
        }
        Schema new_schema(columns);
        Tuple new_tuple(values, &new_schema);
        values_.emplace_back(new_tuple);
      }
    }
  }
  viter_ = values_.begin();
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // first
  if (viter_ != values_.end()) {
    *tuple = *viter_;
    viter_++;
    return true;
  }
  return false;
}

}  // namespace bustub

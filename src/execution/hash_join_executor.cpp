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
    : AbstractExecutor(exec_ctx), 
      plan_(plan), 
      left_child_(left_child.release()),
      right_child_(right_child.release()) {}

void HashJoinExecutor::Init() {
  // Initialize the executors
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  while (left_child_->Next(&tuple, &rid)) {
    Value value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
    JoinKey key = {value};
    join_map_[key].emplace_back(tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) { return false; }
  // first,
}  // namespace bustub

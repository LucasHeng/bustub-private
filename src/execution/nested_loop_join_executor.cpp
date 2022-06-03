//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(left_executor.release()),
      right_executor_(right_executor.release()) {}

void NestedLoopJoinExecutor::Init() {
  // init the left and right executors
  left_executor_->Init();
  // define tuple
  RID rid;
  Tuple l_tuple;
  Tuple r_tuple;
  // form join tuple
  while (left_executor_->Next(&l_tuple, &rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&r_tuple, &rid)) {
      if (plan_->Predicate()
              ->EvaluateJoin(&l_tuple, left_executor_->GetOutputSchema(), &r_tuple, right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        // projection
        // form new schema and new tuple
        std::vector<Column> columns;
        std::vector<Value> values;
        // left
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema()->GetColumnCount(); idx++) {
          values.emplace_back(l_tuple.GetValue(left_executor_->GetOutputSchema(), idx));
          columns.emplace_back(left_executor_->GetOutputSchema()->GetColumn(idx));
        }
        // right
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema()->GetColumnCount(); idx++) {
          values.emplace_back(r_tuple.GetValue(right_executor_->GetOutputSchema(), idx));
          columns.emplace_back(right_executor_->GetOutputSchema()->GetColumn(idx));
        }
        Schema new_schema(columns);
        Tuple new_tuple(values, &new_schema);
        // // project the new tuple
        // std::vector<uint32_t> col_idxs;
        // const Schema *outschema = GetOutputSchema();
        // for (auto &col : outschema->GetColumns()) {
        //   std::string col_name = col.GetName();
        //   uint32_t orig_idx = new_schema.GetColIdx(col_name);
        //   col_idxs.emplace_back(orig_idx);
        // }
        // insert into join tuples vector
        join_tuples_.emplace_back(new_tuple);
      }
    }
  }
  jt_iter_ = join_tuples_.begin();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (jt_iter_ != join_tuples_.end()) {
    *tuple = *jt_iter_;
    jt_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub

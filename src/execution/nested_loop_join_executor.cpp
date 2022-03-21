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
  right_executor_->Init();
  // get the first left tuple
  RID rid;
  left_executor_->Next(&l_tuple, &rid);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple r_tuple;
  while (true) {
    if (!right_executor_->Next(&r_tuple, rid)) {
      if (!left_executor_->Next(&l_tuple, rid)) {
        return false;
      } else {
        right_executor_->Init();
        right_executor_->Next(&r_tuple, rid);
      }
    }
    if (plan_->Predicate()
            ->EvaluateJoin(&l_tuple, left_executor_->GetOutputSchema(), &r_tuple, right_executor_->GetOutputSchema())
            .GetAs<bool>()) {
      // form new schema ans new tuple
      std::vector<Column> columns;
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema()->GetColumnCount(); i++) {
        columns.emplace_back(left_executor_->GetOutputSchema()->GetColumn(i));
        values.emplace_back(l_tuple.GetValue(left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema()->GetColumnCount(); i++) {
        columns.emplace_back(right_executor_->GetOutputSchema()->GetColumn(i));
        values.emplace_back(r_tuple.GetValue(right_executor_->GetOutputSchema(), i));
      }
      Schema new_schema(columns);
      Tuple new_tuple(values, &new_schema);
      // output schema to vector<idx>
      std::vector<uint32_t> col_idxs;
      const Schema *outschema = GetOutputSchema();
      for (uint32_t idx = 0; idx < outschema->GetColumnCount(); idx++) {
        std::string col_name = outschema->GetColumn(idx).GetName();
        uint32_t orig_idx = new_schema.GetColIdx(col_name);
        col_idxs.emplace_back(orig_idx);
      }
      // new tuples
      *tuple = new_tuple.KeyFromTuple(new_schema, *outschema, col_idxs);
      return true;
    }
  }
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(child.release()),
      agg_map_(SimpleAggregationHashTable(plan->GetAggregates(), plan->GetAggregateTypes())),
      mp_begin_(agg_map_.Begin()) {}

void AggregationExecutor::Init() {
  // construct the hash table for the aggregation
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    // group by is null ?
    agg_map_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  mp_begin_ = agg_map_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  LOG_DEBUG("here..");
  while (mp_begin_ != agg_map_.End()) {
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()->EvaluateAggregate(mp_begin_.Key().group_bys_, mp_begin_.Val().aggregates_).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(mp_begin_.Key().group_bys_, mp_begin_.Val().aggregates_));
      }
      *tuple = Tuple(values, GetOutputSchema());
      ++mp_begin_;
      return true;
    } else {
      ++mp_begin_;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      schema_(Schema(std::vector<Column>())),
      table_iter(TableIterator(nullptr, RID(), nullptr)),
      end_(TableIterator(nullptr, RID(), nullptr)) {}

void SeqScanExecutor::Init() {
  TableInfo *table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // table schema
  schema_ = table_info_->schema_;
  // iterator
  table_iter = table_info_->table_.get()->Begin(exec_ctx_->GetTransaction());
  end_ = table_info_->table_.get()->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // expression
  const AbstractExpression *predicate_ = plan_->GetPredicate();
  while (table_iter != end_) {
    const Tuple tmp = *table_iter;
    if (predicate_->Evaluate(&tmp, &schema_).GetAs<bool>()) {
      // get Rid
      *rid = tmp.GetRid();

      // output schema to vector<idx>
      std::vector<uint32_t> col_idxs;
      const Schema *outschema = GetOutputSchema();
      for (uint32_t idx = 0; idx < outschema->GetColumnCount(); idx++) {
        std::string col_name = outschema->GetColumn(idx).GetName();
        uint32_t orig_idx = schema_.GetColIdx(col_name);
        col_idxs.emplace_back(orig_idx);
      }

      // new tuples
      *tuple = table_iter->KeyFromTuple(schema_, *outschema, col_idxs);
      table_iter++;
      return true;
    }
    table_iter++;
  }
  return false;
}

}  // namespace bustub

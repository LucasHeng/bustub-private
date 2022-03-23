//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/expressions/abstract_expression.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

// /**
//  * @brief A simplified hash table that has all the necessary functionality for join
//  * designed by hs
//  */
// class JoinHashTable {
//  public:
//   /** insert new element*/
//   void insert(const Value &key, const Tuple &joinValue) {
//     ht_[key].emplace_back(joinValue);
//   }
//   /** @return Iterator to the target element of the hash table*/
//   std::unordered_map<Value, std::vector<Tuple>>::const_iterator get(const Value &key) const {
//     return ht_[key].cbegin();
//   }
//   /** @return Iterator to the start of the hash table*/
//   std::unordered_map<Value, std::vector<Tuple>>::const_iterator Begin() { return ht_.cbegin(); };
//   /** @return Iterator to the end of the hash table*/
//   std::unordered_map<Value, std::vector<Tuple>>::const_iterator End() { return ht_.cend(); };
//   /** @return Iterator to the start of the hash table value*/
//   std::vector<Tuple>::const_iterator valuesBegin(std::unordered_map<Value, std::vector<Tuple>>::const_iterator iter) {
//     return iter->second.cbegin();  
//   }
//   /** @return Iterator to the end of the hash table value*/
//   std::vector<Tuple>::const_iterator valuesEnd(std::unordered_map<Value, std::vector<Tuple>>::const_iterator iter) {
//     return iter->second.cend();
//   }
//  private:
//   /** the hash table is just a map from join key to a vector of tuple*/
//   std::unordered_map<Value, std::vector<Tuple>> ht_;
// };
/** JoinKey represents a key in an join operation */

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** new element added by hs*/
  std::unordered_map<JoinKey, std::vector<Tuple>> join_map_;
  // the iter of the values
  std::vector<Tuple>::iterator values_;
  // child
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
};

}  // namespace bustub

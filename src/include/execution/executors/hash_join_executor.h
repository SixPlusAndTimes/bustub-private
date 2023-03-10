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

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

// 如何在unordered_map中使用自定义键 ：
// https://stackoverflow.com/questions/17016175/c-unordered-map-using-a-custom-class-type-as-the-key 使用unorder_map
// 则需要自定义 == 和 hash() 两个函数 使用map需要自定义 < 这一个函数
namespace bustub {
struct JoinKey {
  Value value_;

  bool operator==(const JoinKey &other) const { return value_.CompareEquals(other.value_) == CmpBool::CmpTrue; }
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::JoinKey> {
  std::size_t operator()(const bustub::JoinKey &join_key) const {
    size_t curr_hash = 0;
    if (!join_key.value_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&join_key.value_));
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

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
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  // 使用 vector 处理元组有相同jion_key的情况
  // 而不同join_key的碰撞处理，unoreder_map已经实现了，这也是为什么要为JoinKey重载 == 操作符。
  // map的key是连接键，map的value是一个数组，里面存放所有对应连接键的元组
  std::unordered_map<JoinKey, std::vector<Tuple>> join_map_;

  Tuple right_tuple_; // probe阶段的状态变量，相当于右表的游标。 在init中被初始化
  RID right_rid_;

  bool right_table_empty_ = false;
  // 指示在map的vector探测下标
  std::size_t probe_pos_ = 0;  
  // 是否在map的一个vector中探测完成
  bool vector_probe_done_ = false;  
};
}  // namespace bustub

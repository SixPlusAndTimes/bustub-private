//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {
struct DistinKey {
  std::vector<Value> values_;

  bool operator==(const DistinKey &other) const {
    for (uint32_t i = 0; i < other.values_.size(); i++) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {
template <>
struct hash<bustub::DistinKey> {
  std::size_t operator()(const bustub::DistinKey &distinct_key) const {
    size_t curr_hash = 0;
    for (const auto &value : distinct_key.values_) {
      if (!value.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&value));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  // 使用set实现去重， 与 map 和 unorderdmap的区别类似；
  // 如果使用set则要定义“小于”操作符号，或者将comparotor传入模板参数中； 如果使用unorderd_set, 则要定义 == 和 hash
  // 既然 hashjoin使用了 unorderedmap，那么这里就使用 set

  // ！！！！ 不对阿， hashjoin 如果使用 map
  // 那么比较的是一个tuple中的一个joinkey（应该是bustub只支持单个joinkey），但是在distinct中比较的是整个tuple 怎么用 <
  // 比较一个矢量呢？ 没有定义啊。 还是按照AggregateKey的hash自定义键来处理吧
  std::unordered_set<DistinKey> set_;
  std::unordered_set<DistinKey>::const_iterator set_itetator_;
};
}  // namespace bustub

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
#include <utility>
#include <vector>
#include "common/logger.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/page/table_page.h"
#include "storage/table/tuple.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_table_not_empty_ = left_executor_->Next(&left_tuple_, &left_tuple_rid_);
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!left_table_not_empty_) {
    return false;  // 左表为空则直接返回
  }

  bool join_succeed = false;
  Tuple right_tuple;
  RID right_rid;

  while (true) {
    bool got_right_tuple = right_executor_->Next(&right_tuple, &right_rid);
    if (!got_right_tuple) {
      // 右表到底了但是需要先判断左表是否到底，左表到底就直接返回，否则右表会多 next一次，IO_Cost测试不会通过

      // 此时需要更新左边元组
      bool got_left_tuple = left_executor_->Next(&left_tuple_, &left_tuple_rid_);
      if (!got_left_tuple) {
        // 说明左表到底了,连接结束
        return false;
      }
      // LOG_DEBUG("left_next once ____");
      // left_iter_nums++;

      // 右表到底了,初始化右表
      right_executor_->Init();
      bool right_table_not_empty = right_executor_->Next(&right_tuple, &right_rid);
      if (!right_table_not_empty) {
        // 如果初始化之后还是没有得到tuple，说明右表是空的，直接返回false
        return false;
      }
      // right_iter_nums++;
      // LOG_DEBUG("right_next once ____");
    }

    // right_iter_nums++;
    // LOG_DEBUG("right_next once ____");

    if (plan_->Predicate() == nullptr || plan_->Predicate()
                                             ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                            &right_tuple, right_executor_->GetOutputSchema())
                                             .GetAs<bool>()) {
      std::vector<Value> values;
      auto output_shema = plan_->OutputSchema();
      values.reserve(output_shema->GetColumnCount());
      for (uint32_t i = 0; i < output_shema->GetColumnCount(); i++) {
        Value value = output_shema->GetColumn(i).GetExpr()->EvaluateJoin(
            &left_tuple_, left_executor_->GetOutputSchema(), &right_tuple, right_executor_->GetOutputSchema());
        values.push_back(value);
      }
      *tuple = Tuple(values, output_shema);
      *rid = left_tuple_.GetRid();  // 左表的rid
      join_succeed = true;
      return join_succeed;
    }
  }
  return join_succeed;
}

}  // namespace bustub

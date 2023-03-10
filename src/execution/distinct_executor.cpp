//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"
#include <cstdint>
#include <vector>
#include "catalog/schema.h"
#include "common/logger.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  set_.clear();
  Tuple tuple;
  RID rid;
  auto schema = plan_->OutputSchema();
  DistinKey distinct_key;
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      values.push_back(tuple.GetValue(schema, i));
    }
    // 优化点： std::move
    set_.insert({std::move(values)});

  }
  set_itetator_ = set_.cbegin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (set_itetator_ != set_.cend()) {
    // 优化点 ： 不需要临时变量, 迭代器自增操作改成前置
    *tuple = Tuple((*set_itetator_).values_, plan_->OutputSchema());
    ++set_itetator_;
    return true;
  }
  return false;
}

}  // namespace bustub

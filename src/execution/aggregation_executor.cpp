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
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::TupleSchemaTranformUseEvaluateAggregate(const std::vector<Value> &group_bys,
                                                                  const std::vector<Value> &aggregates,
                                                                  Tuple *dest_tuple, const Schema *dest_schema) {
  auto& colums = dest_schema->GetColumns();
  std::vector<Value> dest_value;
  // 优化点 ： reserve
  dest_value.reserve(colums.size());
  for (const auto &col : colums) {
    dest_value.push_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
  }
  *dest_tuple = Tuple(dest_value, dest_schema);
}
// 优化点：消除临时变量
void AggregationExecutor::Init() {
  child_->Init();

  Tuple child_tuple;
  RID child_rid;
  while (true) {
    if (!child_->Next(&child_tuple, &child_rid)) {
      break;
    }

    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  auto output_schema = plan_->OutputSchema();
  auto having_exr = plan_->GetHaving();
  bool having_res;
  while (aht_iterator_ != aht_.End()) {
    having_res = true;
    if (having_exr != nullptr) {
      having_res =
          having_exr->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>();
    }

    if (having_res) {
      TupleSchemaTranformUseEvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_, tuple,
                                              output_schema);
      // *rid = tuple->GetRid(); 合成的元组没有RID，没必要赋值
      ++aht_iterator_;  // 指向下一位置
      return true;
    }
    ++aht_iterator_;  // 指向下一位置
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub

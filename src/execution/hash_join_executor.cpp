//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cassert>
#include <vector>
#include "catalog/schema.h"
#include "common/logger.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  join_map_.clear();
  if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
    // 右表为空
    right_table_empty_ = true;
  }
  // Build阶段在Init中完成， 即将外表(左表)的全部元组按照joinkey散列到hash_map中
  Tuple left_tuple;
  RID left_rid;
  Value left_join_value;
  JoinKey left_join_key;
  const Schema *left_schema = left_executor_->GetOutputSchema();
  // 优化点： find 和 count的使用，std::move, 中间变量完全可以不要， hashmap的chain为什么用vector不用list？
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    left_join_value = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);
    left_join_key = {left_join_value};
    if (join_map_.count(left_join_key) == 0) {
      join_map_.insert({left_join_key, std::vector<Tuple>{left_tuple}});
    } else {
      // 注意这里必须是std::vector<Tuple>&，必须取引用而不是复制，否则 在处理重复key时会出错！
      // std::vector<Tuple> tuple_vector = join_map_[left_join_key];  这样就是复制了一个vector
      std::vector<Tuple> &tuple_vector = join_map_[left_join_key];
      tuple_vector.push_back(left_tuple);
    }
  }
  // int num = 0;
  // for (auto & pair : join_map_) {
  //     int num_vec = static_cast<int>(pair.second.size());
  //     num += static_cast<int>(pair.second.size());
  //     LOG_DEBUG("hash map vector size %d", num_vec);
  // }
  // LOG_DEBUG("hash map size %d", num );
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // probe 阶段

  if (join_map_.empty() || right_table_empty_) {
    // 外表或者内表为空直接返回false
    return false;
  }
  Value right_join_value;
  JoinKey right_join_key;
  const Schema *right_schema = right_executor_->GetOutputSchema();
  // 优化点： 中间变量完全可以不要
  right_join_value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_schema);
  right_join_key = {right_join_value};
  while (join_map_.count(right_join_key) == 0 || vector_probe_done_) {
    // 左表没有对应的key，next取得下一个右表元组并保存与本类的数据成员中
    bool right_table_probe_not_done = right_executor_->Next(&right_tuple_, &right_rid_);
    if (!right_table_probe_not_done) {
      return false;  // 右表结束了探测
    }
    // 更新rightjoinkey， 和probe_pos_
    right_join_value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_schema);
    right_join_key = {right_join_value};
    probe_pos_ = 0;
    vector_probe_done_ = false;
  }
  // 优化点，使用引用
  auto tuple_vector = join_map_[right_join_key];
  auto tuple_vector_size = tuple_vector.size();

  // 到这里，探测游标一定是小于数组大小的。
  assert(probe_pos_ < tuple_vector_size); 

  auto left_tuple_in_map = tuple_vector[probe_pos_];
  probe_pos_++;
  if (probe_pos_ == tuple_vector_size) {
    vector_probe_done_ = true;  // 这个vector已经探测完成
  }

  // 组装连接后的tuple
  auto out_schema = plan_->OutputSchema();
  auto left_schema = left_executor_->GetOutputSchema();
  // auto right_schema = right_executor_->GetOutputSchema();

  auto colunms = out_schema->GetColumns();
  std::vector<Value> out_values;
  // 优化点：， reserve 和 push back ？
  out_values.reserve(colunms.size());
  for (const auto &col : colunms) {
    out_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_in_map, left_schema, &right_tuple_, right_schema));
  }
  // 为输出参数复制
  *tuple = Tuple(out_values, out_schema);
  return true;
}

}  // namespace bustub

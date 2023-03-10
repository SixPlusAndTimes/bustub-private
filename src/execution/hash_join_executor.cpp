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
#include <cstddef>
#include <vector>
#include "catalog/schema.h"
#include "common/logger.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {
// 分两个阶段
// 1. build阶段，根据连接键，将左表的元组散列到hash表
// 2. probe阶段，执行join
// 3. 使用两个探测指针，一个指针指向hash表vector的元素， 另一个指向右表的记录
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
  // 初始化right_tuple_这个状态变量
  if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
    // 右表为空
    right_table_empty_ = true;
  }
  // 1. Build阶段在Init中完成， 即将外表(左表)的全部元组按照joinkey散列到hash_map中
  Tuple left_tuple;
  RID left_rid;
  JoinKey left_join_key;
  const Schema *left_schema = left_executor_->GetOutputSchema();
  // 优化点： find 和 count的使用，std::move, 中间变量完全可以不要， map使用emplace？，hashmap的chain为什么用vector不用list？
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    left_join_key = {plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema)};
    auto iter = join_map_.find(left_join_key);
    if (iter == join_map_.end()) {
      join_map_.emplace(left_join_key, std::vector<Tuple>{left_tuple});
    } else {
      iter->second.push_back(std::move(left_tuple));
    }
  }

}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // 2. probe 阶段
  
  // 2.1 外表或者内表为空直接返回false
  if (join_map_.empty() || right_table_empty_) {
    return false;
  }

  JoinKey right_join_key;
  const Schema *right_schema = right_executor_->GetOutputSchema();
  auto iter = join_map_.end();
  // 优化点： 中间变量完全可以不要
  // 2.1 循环找出hash表中与右表元组连接键相同的左表元组的vector。right_tuple_相当于指向右表的元组。 如果找不到与之相同连接键的左表元组，则更新right_tuple_指向下一个右表元组
  right_join_key = {plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_schema)};
  while ((iter = join_map_.find(right_join_key)) == join_map_.end() || vector_probe_done_) {
    // 左表没有对应的key，next取得下一个右表元组并更新右表的游标right_tuple_
    if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
      return false;  // 这表示右表的游标已经指向了最后一个记录，结束join操作
    }
    // 更新rightjoinkey， 初始化probe_pos_
    right_join_key = {plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_schema)};
    probe_pos_ = 0;
    vector_probe_done_ = false;
  }

  // 2.2 选出由左表元组组成的vector中的一个元组，由probe_pos_这个探测指针指定。然后更新probe_pos_
  // 优化点，直接使用迭代器
  auto tuple_vector = iter->second;

  size_t tuple_vector_size = tuple_vector.size();
  // 到这里，探测游标一定是小于数组大小的。
  assert(probe_pos_ < tuple_vector_size); 

  auto left_tuple_in_map = tuple_vector[probe_pos_];

  if (++probe_pos_ == tuple_vector_size) {
    vector_probe_done_ = true;  // 这个vector已经探测完成
  }

  // 2.3 组装连接后的tuple
  auto out_schema = plan_->OutputSchema();
  auto left_schema = left_executor_->GetOutputSchema();
  // 优化点：使用引用
  auto& colunms = out_schema->GetColumns();
  std::vector<Value> out_values;
  // 优化点：使用reserve 
  out_values.reserve(colunms.size());
  for (const auto &col : colunms) {
    out_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_in_map, left_schema, &right_tuple_, right_schema));
  }
  // 为输出参数赋值
  *tuple = Tuple(out_values, out_schema);
  return true;
}

}  // namespace bustub

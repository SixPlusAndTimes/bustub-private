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
#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      predicate_(plan_->GetPredicate()),
      tableheap_iterator_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      output_shema_(plan_->OutputSchema()) {}

// seq_scan 没有子计划节点
void SeqScanExecutor::Init() {
  // 获取对应TableHeap的迭代器 和 TableInfo
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  tableheap_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  bool successed = false;
  while (tableheap_iterator_ != table_info_->table_->End()) {
    // &(*tableheap_iterator_) : 对迭代器解引用得到 Tuple& , 再对Tuple取地址得到 Tuple *
    if (predicate_ != nullptr &&
        !predicate_->Evaluate(&(*tableheap_iterator_), &(table_info_->schema_)).GetAs<bool>()) {
      // 不满足predicate
      ++tableheap_iterator_;
      continue;
    }

    std::vector<Value> values;
    values.reserve(output_shema_->GetColumnCount());
    for (uint32_t i = 0; i < output_shema_->GetColumnCount(); i++) {
      Value value = output_shema_->GetColumn(i).GetExpr()->Evaluate(&(*tableheap_iterator_), output_shema_);
      values.push_back(value);
    }
    successed = true;
    // 创建Tuple并将其复制到tuple指向的地址

    /** 下面的new 操作会在堆内存中申请空间，但是没有对应的析构函数
     tuple = new Tuple(values, output_shema_); 这一行有两个错误，1是没有析构Tuple
    ,2是没有在给定的地址创建，请见execution.h中的执行代码。 new(tuple)Tuple(values, output_shema_);
    这一行也错，错在在堆上分配对象
    **/
    *tuple = Tuple(values, output_shema_);

    *rid = tuple->GetRid();
    ++tableheap_iterator_;
    return successed;
  }
  return successed;
}

}  // namespace bustub

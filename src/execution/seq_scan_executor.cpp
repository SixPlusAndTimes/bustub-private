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
#include <ostream>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tableheap_iterator_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr),
      output_shema_(plan_->OutputSchema()) {}

// seq_scan 没有子计划节点
void SeqScanExecutor::Init() {
  // 获取对应TableHeap的迭代器 和 TableInfo
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  tableheap_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

// 注意 ： NEXT输出的元组并不等于表元组，它是没有RID的； 其RID只能从对应的表元组中得到
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (tableheap_iterator_ != table_info_->table_->End()) {

    //除了 READ_UNCOMMITER 隔离级别，其他级别都需要加读锁
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      exec_ctx_->GetLockManager()->LockShared(exec_ctx_->GetTransaction(), tableheap_iterator_->GetRid());
      // LOG_DEBUG("lockshared txn id %d, ridpageid = %d, ridslotnum =
      // %d",exec_ctx_->GetTransaction()->GetTransactionId(),tableheap_iterator_->GetRid().GetPageId(),
      // tableheap_iterator_->GetRid().GetSlotNum() );
    }
    auto predicate = plan_->GetPredicate();
    if (predicate != nullptr && !predicate->Evaluate(&(*tableheap_iterator_), &(table_info_->schema_)).GetAs<bool>()) {
      // 不满足predicate, 且如果隔离等级为read_commited 则立刻释放锁
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), tableheap_iterator_->GetRid());
      }
      ++tableheap_iterator_;
      continue;
    }

    // 构造ValueVector以创建Tuple
    std::vector<Value> values;
    // values.reserve(output_shema_->GetColumnCount());
    for (uint32_t i = 0; i < output_shema_->GetColumnCount(); i++) {
      // 注意 ： 这里的Evaluate方法输入的schema参数是锁扫描表的shema，而不是outputschema，否则会访问非法内存！
      Value value = output_shema_->GetColumn(i).GetExpr()->Evaluate(&(*tableheap_iterator_), &table_info_->schema_);
      values.push_back(value);
    }

    // 创建Tuple并传出
    /** 下面的new 操作会在堆内存中申请空间，但是没有对应的析构函数
     tuple = new Tuple(values, output_shema_); 这一行有两个错误，1是没有析构Tuple
    ,2是没有在给定的地址创建，请见execution.h中的执行代码。 new(tuple)Tuple(values, output_shema_);
    这一行也错，错在在堆上分配对象
    **/
    // 必须使用copy assignment进行赋值
    *tuple = Tuple(values, output_shema_);
    // *rid = tuple->GetRid();  错误，输出元组是没有RID这个说法的
    *rid = tableheap_iterator_->GetRid();  // 只能从表元组中得到对应的RID
    
    // 如果隔离等级为read_commited 则读完立刻释放锁
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), tableheap_iterator_->GetRid());
    }
    ++tableheap_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub

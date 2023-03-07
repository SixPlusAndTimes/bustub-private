//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <iterator>
#include <list>
#include <mutex>  // NOLINT
#include <ostream>
#include <tuple>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {
// 注意： 普通2PL只能解决不可重复读的问题，不能解决脏读问题； 严格而阶段锁能够解决脏读问题
//
// isolation = READ_UNCOMMITTED : 不需要读锁， 只需要写锁
// isolation = READ_COMMITED : 读锁，写锁都需要，但是读锁 在读完就释放， 写时上写锁，在commit时才释放写锁 (不需要2pl)
// isolation = REPEATABLE_READ ：
// 使用严格二阶段锁，在growing阶段只能获取锁，在shring阶段只能释放锁，而且所有的锁都在commit时才释放 isolation =
// SERIALIZABLE ： 本lab不要求，理论上需要 strict 2PL + index locks

// 读写锁立即释放还是在commit时释放，不由lock_manager决定，而是由各executor决定

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  auto islation_level = txn->GetIsolationLevel();
  //  read uncomited 隔离级别只需写锁，不需要加读锁
  if (islation_level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 强两阶段锁判定，必须在growing阶段才能加锁，否则事务Abort
  if (islation_level == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 使用 unique_lock 加锁，不需要在return前解锁，而且std::condition_variable 必须配合std::unique_lock来使用
  std::unique_lock<std::mutex> uniq_lk(latch_);
  txnid_to_txnptr_map_.emplace(txn->GetTransactionId(), txn); // 为了方便wound-wait算法，把事务对象的指针放入一个map
  LockRequest new_request(txn->GetTransactionId(), LockMode::SHARED);
  if (lock_table_.count(rid) == 0) {
    // lock_table_ 中还没有这个RID对应的锁的化创建它
    // 由于 mutex 不能复制，所以要使用stl的emplace接口， 但我们使用的容器是map，所以又会有一些特殊的地方
    //   1. unordered_map由key，value组成，无法区分emplce()参数中，那些用来构造key，那些用来构造alue，因此可以使用makepair或者forward_as_tuple做区分
    //   2. 但是这里的unordered_map的alue中有一个LockRequestQueue内含条件变量，它不能被拷贝也不能被移动，必须在原地构造出来，因此传入了piecewise_construct参数
    //   3. 下面的创建语法就是说rid作为key， 而value则是原地构建出来的。 
    // 详见https://www.cnblogs.com/guxuanqing/p/11396511.html
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    // 在新创建的requestqueue添加新的 request
    new_request.granted_ = true;
    lock_table_[rid].request_queue_.push_back(new_request);
    lock_table_[rid].sharing_count_++;
  } else {
    LockRequestQueue &request_queue = lock_table_[rid];
    // LOG_DEBUG("txn_id = %d , lockshared has request_queue", txn->GetTransactionId());
    // wound wait(young wait for old, old kill young)算法 预防死锁, 读锁共存，写读互斥
    bool exist_writer = false;
    for (auto &request : request_queue.request_queue_) {
      // 检查等待队列中的新事务是否要获取写锁
      if (request.txn_id_ > txn->GetTransactionId() && request.lock_mode_ == LockMode::EXCLUSIVE) {
        // 新事务abort
        request.granted_ = false;
        txnid_to_txnptr_map_[request.txn_id_]->SetState(TransactionState::ABORTED);
        // LOG_DEBUG("txn id = %d abored by txn id = %d", request.txn_id_, txn->GetTransactionId());
        // 唤醒被wound wait算法abort的事务
        request_queue.cv_.notify_all();
      }else if (request.txn_id_ < txn->GetTransactionId()){
        // 如果改事务比本事务老，则判断它是否是读锁
        if (request.lock_mode_ == LockMode::EXCLUSIVE) {
          exist_writer = true;
        }
      }else if(request.txn_id_ ==txn->GetTransactionId()){
        if (request.granted_) 
        { 
          return true;
        }
      }
    }
    request_queue.has_writer_ = exist_writer;
    request_queue.request_queue_.push_back(new_request);
    // 等待这个RID的请求队列中没有写锁
    while (request_queue.has_writer_ ) {
      // 如果 被abort 或者 并发条件满足则break
      if (txn->GetState() == TransactionState::ABORTED) {
        break;
      }
      request_queue.cv_.wait(uniq_lk);
    }

    // 遍历request_list
    std::list<LockRequest>::iterator itetator_list = GetIterator(request_queue,txn->GetTransactionId());
    // 本事务在等待过程中可能被abort
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue.request_queue_.erase(itetator_list);
      return false;
    }
    // 获得了锁
    (*itetator_list).granted_ = true;
    request_queue.sharing_count_++;
  }
  // 将获得锁的记录存放在事务对象中
  txn->GetSharedLockSet()->emplace(rid);
  
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 两阶段锁判定， 在加锁阶段事物状态必须是 GROWING
  // islation_level == IsolationLevel::REPEATABLE_READ 这个条件不加好像也可以
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  std::unique_lock<std::mutex> uniq_lk(latch_);

  txnid_to_txnptr_map_.emplace(txn->GetTransactionId(), txn);

  LockRequest new_request(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  if (lock_table_.count(rid) == 0) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    lock_table_[rid].request_queue_.push_back(new_request);
    lock_table_[rid].has_writer_ = true;
  } else {
    LockRequestQueue &request_queue = lock_table_[rid];

    WoundWait(request_queue, txn->GetTransactionId());
    request_queue.request_queue_.push_back(new_request);  // 先将请求加入队列
    // 等待队列中没有（老事务的）读锁（这样似乎读者会饿死写者？）, 或者被 aborte
    while ((request_queue.sharing_count_ > 0 || request_queue.has_writer_)) {
      // 下面的if条件是判断自己是否被abort了，如果是就退出循环
      if (txn->GetState() == TransactionState::ABORTED ) {
        break;
      }
      // LOG_DEBUG("txn id = %d waitin ", txn->GetTransactionId());
      request_queue.cv_.wait(uniq_lk);
    }

    std::list<LockRequest>::iterator itetator_list = GetIterator(request_queue, txn->GetTransactionId());
    // 在等待过程中可能被杀死
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue.request_queue_.erase(itetator_list);
      return false;
    }

    (*itetator_list).granted_ = true;
    request_queue.has_writer_ = true;
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 两阶段锁判定， 在加锁阶段事物状态必须是 GROWING
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  std::unique_lock<std::mutex> uniq_lk(latch_);
  LockRequestQueue &request_queue = lock_table_[rid];
  // 如果有正在升级的请求则abort
  if (request_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
  }
  txn->GetSharedLockSet()->erase(rid);
  request_queue.sharing_count_--;

  std::list<LockRequest>::iterator itetator_list = GetIterator(request_queue, txn->GetTransactionId());
  (*itetator_list).granted_ = false;
  request_queue.upgrading_ = true; // 表示正在锁升级
  // wound wait算法 预防死锁
  bool exixt_writer = false;
  for (auto &request : request_queue.request_queue_) {
    if (request.txn_id_ > txn->GetTransactionId()) {
      // 新事务abort
      if (request.lock_mode_ == LockMode::SHARED) {
        request_queue.sharing_count_--;
      } 
      request.granted_ = false;
      txnid_to_txnptr_map_[request.txn_id_]->SetState(TransactionState::ABORTED);
    }else {
      if (request.lock_mode_ == LockMode::EXCLUSIVE) {        
        exixt_writer = true;
      }
    }
    request_queue.cv_.notify_all();
  }
  request_queue.has_writer_ = exixt_writer;
  // 等待
  while (request_queue.has_writer_ || request_queue.sharing_count_ > 0 ) {
    // 下面的if条件是判断自己是否在等待期间被abort了，如果是就推出循环
    if (txn->GetState() == TransactionState::ABORTED) {
      break;
    }
    request_queue.cv_.wait(uniq_lk);
  }
  // 在等待过程中可能被杀死
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.request_queue_.erase(itetator_list);
    return false;
  }

  (*itetator_list).granted_ = true;
  (*itetator_list).lock_mode_ = LockMode::EXCLUSIVE;
  request_queue.has_writer_ = true;
  request_queue.upgrading_ = false;
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> unique_lk(latch_);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  auto txn_id = txn->GetTransactionId();
  // 两阶段锁处理, 在 repeatable 隔离级别下才将事务状态改为 Shrinking
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 找到这个事务对应的 LockRequest
  LockRequestQueue &request_queue = lock_table_[rid];
  std::list<LockRequest>::iterator itetator_list = GetIterator(request_queue, txn_id);

  auto lock_mode = (*itetator_list).lock_mode_;  // 获取lockmode后在list中删除这个LcokRequest
  if (itetator_list != request_queue.request_queue_.end()) {
    request_queue.request_queue_.erase(itetator_list);
  }

  /*唤醒其他可能的请求*/
  if (lock_mode == LockMode::SHARED) {
    request_queue.sharing_count_--;
    if (request_queue.sharing_count_ == 0) {
      // 唤醒写请求
      request_queue.cv_.notify_all();
    }
  } else {
    // 唤醒读请求
    request_queue.has_writer_ = false;
    request_queue.cv_.notify_all();
  }
  return true;
}

// helper function
// wound wait 算法：
void LockManager::WoundWait(LockRequestQueue& request_queue,txn_id_t upcoming_trx_id) {
    int sharing_count = 0;
    bool has_wrtiter = false;
    for (auto &request : request_queue.request_queue_) {
      if (request.txn_id_ > upcoming_trx_id) {
        // 新事务abort
        request.granted_ = false;
        txnid_to_txnptr_map_[request.txn_id_]->SetState(TransactionState::ABORTED);
        request_queue.cv_.notify_all();
      } else {
        if (request.lock_mode_ == LockMode::SHARED) {
          sharing_count++;
        } else {
          has_wrtiter = true;
        }
      }
    }
    request_queue.has_writer_ = has_wrtiter;
    request_queue.sharing_count_ = sharing_count;
}
// 遍历requestqueue获得相应的迭代其
std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(LockRequestQueue& request_queue,txn_id_t txn_id ){
  std::list<LockRequest>::iterator itetator_list;
  for (itetator_list = request_queue.request_queue_.begin(); itetator_list != request_queue.request_queue_.end();
       ++itetator_list) {
    if ((*itetator_list).txn_id_ == txn_id) {
      break;
    }
  }
  return itetator_list;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}



}  // namespace bustub

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

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
bool LockManager::Check(LockMode mode, const RID &rid, txn_id_t *tid) {
  // is there the lockrequestqueue of rid
  if (lock_table_.find(rid) == lock_table_.end()) {
    return true;
  }
  auto &lrq = lock_table_[rid];
  if (mode == LockMode::SHARED) {
    // check if exist exclusive lock request
    for (auto &request : lrq.request_queue_) {
      if (request.lock_mode_ == LockMode::EXCLUSIVE && request.granted_) {
        *tid = request.txn_id_;
        return false;
      }
    }
    return true;
  }
  // if upgrade
  if (lrq.upgrading_ != INVALID_TXN_ID) {
    for (auto &request : lrq.request_queue_) {
      if (request.granted_ && request.txn_id_ != lrq.upgrading_) {
        *tid = request.txn_id_;
        return false;
      }
    }
    return true;
  }
  for (auto &request : lrq.request_queue_) {
    if (request.granted_) {
      *tid = request.txn_id_;
      return false;
    }
  }
  return true;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  // chech the txn state
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    return true;
  }
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  //
  while (true) {
    // 可能有别的事务杀死它
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    bool is_continue = true;
    for (auto &lrq : lock_table_[rid].request_queue_) {
      if (lrq.lock_mode_ == LockMode::EXCLUSIVE) {
        // 如果本地事务更早，就abort掉正在执行的
        if (lrq.txn_id_ > txn->GetTransactionId()) {
          lrq.granted_ = false;
          TransactionManager::txn_map[lrq.txn_id_]->SetState(TransactionState::ABORTED);
        } else {
          // 存在比它还早的事务，就等着吧
          is_continue = false;
          break;
        }
      }
    }
    if (is_continue) {
      // 前面判断是否有比它更早的事务
      break;
    }
    lock_table_[rid].cv_.wait(lock);
  }
  // //
  // lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
  // while (!Check(LockMode::SHARED, rid, &tid)) {
  //   bool wound = false;
  //   if (tid > txn->GetTransactionId()) {
  //     for (auto &request : lock_table_[rid].request_queue_) {
  //       if (request.lock_mode_ == LockMode::EXCLUSIVE && request.txn_id_ == tid) {
  //         request.granted_ = false;
  //         TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
  //         wound = true;
  //       }
  //     }
  //   }
  //   if (wound) {
  //     continue;
  //   }
  //   lock_table_[rid].cv_.wait(lock);
  // }
  // for (auto &lrq : lock_table_[rid].request_queue_) {
  //   if (lrq.txn_id_ == txn->GetTransactionId()) {
  //     lrq.granted_ = true;
  //     break;
  //   }
  // }
  LockRequest request(txn->GetTransactionId(), LockMode::SHARED);
  request.granted_ = true;
  lock_table_[rid].request_queue_.emplace_back(request);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // check the txn state
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  for (auto &lrq : lock_table_[rid].request_queue_) {
    // 写的话是所有的正在运行的事务都比较
    // 如果本地事务更早，就abort掉正在执行的
    if (lrq.txn_id_ > txn->GetTransactionId() || txn->GetTransactionId() == 9) {
      lrq.granted_ = false;
      TransactionManager::txn_map[lrq.txn_id_]->SetState(TransactionState::ABORTED);
    } else {
      // 存在比它还早的事务，就等着吧
      TransactionManager::txn_map[txn->GetTransactionId()]->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  // lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  // while (!Check(LockMode::EXCLUSIVE, rid, &tid)) {
  //   // check wound-wait
  //   bool flag = false;
  //   if (tid > txn->GetTransactionId()) {
  //     if (tid == lock_table_[rid].upgrading_) {
  //       lock_table_[rid].upgrading_ = INVALID_TXN_ID;
  //     }
  //     for (auto &lrq : lock_table_[rid].request_queue_) {
  //       if (lrq.txn_id_ == tid) {
  //         lrq.granted_ = false;
  //         TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
  //         flag = true;
  //       }
  //     }
  //   }
  //   if (flag) {
  //     continue;
  //   }
  //   lock_table_[rid].cv_.wait(lock);
  // }
  // for (auto &lrq : lock_table_[rid].request_queue_) {
  //   if (lrq.txn_id_ == txn->GetTransactionId()) {
  //     lrq.granted_ = true;
  //     break;
  //   }
  // }
  LockRequest request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  request.granted_ = true;
  lock_table_[rid].request_queue_.emplace_back(request);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // lock
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID && lock_table_[rid].upgrading_ != txn->GetTransactionId()) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  assert(txn->IsSharedLocked(rid));
  if (txn->IsExclusiveLocked(rid)) {
    return false;
  }
  lock_table_[rid].upgrading_ = txn->GetTransactionId();
  while (true) {
    // 可能有别的事务杀死它
    if (txn->GetState() == TransactionState::ABORTED) {
      return false;
    }
    bool is_continue = true;
    for (auto &lrq : lock_table_[rid].request_queue_) {
      // 写的话是所有的正在运行的事务都比较,除了自己本身的读事务
      if (lrq.txn_id_ != txn->GetTransactionId()) {
        // 如果本地事务更早，就abort掉正在执行的
        if (lrq.txn_id_ > txn->GetTransactionId()) {
          lrq.granted_ = false;
          TransactionManager::txn_map[lrq.txn_id_]->SetState(TransactionState::ABORTED);
        } else {
          // 存在比它还早的事务，就等着吧
          is_continue = false;
          break;
        }
      }
    }
    if (is_continue) {
      // 前面判断是否有比它更早的事务
      break;
    }
    lock_table_[rid].cv_.wait(lock);
  }

  // while (!Check(LockMode::EXCLUSIVE, rid, &tid)) {
  //   bool flag = false;
  //   if (tid > txn->GetTransactionId()) {
  //     for (auto &lrq : lock_table_[rid].request_queue_) {
  //       if (lrq.txn_id_ == tid) {
  //         lrq.granted_ = false;
  //         TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
  //         flag = true;
  //       }
  //     }
  //   }
  //   if (flag) {
  //     continue;
  //   }
  //   lock_table_[rid].cv_.wait(lock);
  // }
  // 找到自己的读事务，升级，由于前面的限制，所以不可能有两个读事务
  for (auto &lrq : lock_table_[rid].request_queue_) {
    if (lrq.txn_id_ == txn->GetTransactionId()) {
      lrq.lock_mode_ = LockMode::EXCLUSIVE;
      lock_table_[rid].upgrading_ = INVALID_TXN_ID;
      lrq.granted_ = true;
      break;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // lock
  std::unique_lock<std::mutex> lock(latch_);
  // if (txn->GetState() == TransactionState::SHRINKING) {
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UNLOCK_ON_SHRINKING);
  // }
  // 除了read_commit,应该都应该改状态，因为read commit提前unlock，对于read uncommit,它只有写锁
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
    txn->SetState(TransactionState::SHRINKING);
  }
  for (auto iter = lock_table_[rid].request_queue_.begin(); iter != lock_table_[rid].request_queue_.end(); iter++) {
    // 撤销对应rid对应事务的锁，只有一个
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_table_[rid].request_queue_.erase(iter);
      lock_table_[rid].cv_.notify_all();
      break;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub

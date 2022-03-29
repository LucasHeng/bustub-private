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
    // is upgrading, share should be blocked
    if (lrq.upgrading_ != INVALID_TXN_ID) {
      return false;
    }
    // check if exist exclusive lock request
    for (auto &request : lrq.request_queue_) {
      if (request.lock_mode_ == LockMode::EXCLUSIVE && request.granted_) {
        *tid = request.txn_id_;
        return false;
      }
    }
    return true;
  }
  // is upgrading, exclusive should be blocked
  // if (lrq.upgrading_ != INVALID_TXN_ID && lrq.upgrading_ != txn->GetTransactionId()) {
  //   return false;
  // }
  // check if exist lock request granted
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
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  txn_id_t tid = INVALID_TXN_ID;
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
        return true;
      }
      //
      lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
      while (!Check(LockMode::SHARED, rid, &tid)) {
        bool wound = false;
        if (tid > txn->GetTransactionId()) {
          for (auto &request : lock_table_[rid].request_queue_) {
            if (request.lock_mode_ == LockMode::EXCLUSIVE && request.txn_id_ == tid) {
              request.granted_ = false;
              TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
              wound = true;
            }
          }
        }
        if (wound) {
          continue;
        }
        lock_table_[rid].cv_.wait(lock);
      }
      for (auto &lrq : lock_table_[rid].request_queue_) {
        if (lrq.txn_id_ == txn->GetTransactionId()) {
          lrq.granted_ = true;
          break;
        }
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
        return true;
      }
      //
      lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED);
      while (!Check(LockMode::SHARED, rid, &tid)) {
        bool wound = false;
        if (tid > txn->GetTransactionId()) {
          for (auto &request : lock_table_[rid].request_queue_) {
            if (request.lock_mode_ == LockMode::EXCLUSIVE && request.txn_id_ == tid) {
              request.granted_ = false;
              TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
              wound = true;
            }
          }
        }
        if (wound) {
          continue;
        }
        lock_table_[rid].cv_.wait(lock);
      }
      for (auto &lrq : lock_table_[rid].request_queue_) {
        if (lrq.txn_id_ == txn->GetTransactionId()) {
          lrq.granted_ = true;
          break;
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      break;
    default:
      break;
  }
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
  txn_id_t tid = INVALID_TXN_ID;
  if (txn->IsSharedLocked(rid)) {
    return LockUpgrade(txn, rid);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  lock_table_[rid].request_queue_.emplace_back(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  while (!Check(LockMode::EXCLUSIVE, rid, &tid)) {
    // check wound-wait
    bool flag = false;
    if (tid > txn->GetTransactionId()) {
      if (tid == lock_table_[rid].upgrading_) {
        lock_table_[rid].upgrading_ = INVALID_TXN_ID;
      }
      for (auto &lrq : lock_table_[rid].request_queue_) {
        if (lrq.txn_id_ == tid) {
          lrq.granted_ = false;
          TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
          flag = true;
        }
      }
    }
    if (flag) {
      continue;
    }
    lock_table_[rid].cv_.wait(lock);
  }
  for (auto &lrq : lock_table_[rid].request_queue_) {
    if (lrq.txn_id_ == txn->GetTransactionId()) {
      lrq.granted_ = true;
      break;
    }
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // lock
  std::unique_lock<std::mutex> lock(latch_);
  // if (txn->GetState() != TransactionState::GROWING) {
  //   txn->SetState(TransactionState::ABORTED);
  //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  //   return false;
  // }
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID && lock_table_[rid].upgrading_ != txn->GetTransactionId()) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  txn_id_t tid = INVALID_TXN_ID;
  assert(txn->IsSharedLocked(rid));
  if (txn->IsExclusiveLocked(rid)) {
    return false;
  }
  lock_table_[rid].upgrading_ = txn->GetTransactionId();
  while (!Check(LockMode::EXCLUSIVE, rid, &tid)) {
    LOG_DEBUG("I/O error");
    if (tid == txn->GetTransactionId()) {
      break;
    }
    bool flag = false;
    if (tid > txn->GetTransactionId()) {
      for (auto &lrq : lock_table_[rid].request_queue_) {
        if (lrq.txn_id_ == tid) {
          lrq.granted_ = false;
          TransactionManager::txn_map[tid]->SetState(TransactionState::ABORTED);
          flag = true;
        }
      }
    }
    if (flag) {
      continue;
    }
    lock_table_[rid].cv_.wait(lock);
  }
  for (auto &lrq : lock_table_[rid].request_queue_) {
    if (lrq.txn_id_ == txn->GetTransactionId()) {
      lrq.lock_mode_ = LockMode::EXCLUSIVE;
      lock_table_[rid].upgrading_ = INVALID_TXN_ID;
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
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
    txn->SetState(TransactionState::SHRINKING);
  }
  for (auto iter = lock_table_[rid].request_queue_.begin(); iter != lock_table_[rid].request_queue_.end(); iter++) {
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

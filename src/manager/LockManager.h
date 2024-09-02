#pragma once

#include <unordered_map>
#include <string>
#include <mutex>
#include <condition_variable>
#include <queue>

#include "../common.h"
/*
  If call get on a scan
  -> get slock.


  if call write
  -> get xlock on current block
  -> get xlock on the next block that is written to...

  Wait-Die

  Ti wants a lock that Tj has
  Ti has lower priority -> Ti dies
  Ti has higher priority -> Ti waits

  Slock can have multiple reader, cannot write

  // need to be able to upgrade SLock to XLock
*/

enum LockStatus {
  SLock,
  XLock
};

struct LockMetaData {
  LockStatus status;
  u32 count;
  std::time_t earliestTime;
};

class LockManager {
private:
  std::unordered_map<std::string, std::unordered_map<u64, LockMetaData>> lockMap;
  std::mutex mut;
  std::condition_variable cv;
  u32 defaultWaitTime;
public:

  LockManager(u32 wt = 5) : lockMap{}, mut{}, cv{}, defaultWaitTime{ wt } {}

  LockMetaData& getLockMetaData(PageId& pageId) {
    return lockMap.at(pageId.filename).at(pageId.pageNumber);
  }

  // release shared lock
  bool releaseSLock(PageId& pageId) {
    {
      std::lock_guard<std::mutex> lk(mut);

      auto& pageMap = this->lockMap.at(pageId.filename);
      LockMetaData& metadata = pageMap.at(pageId.pageNumber);
      metadata.count -= 1;
      if (metadata.count == 0) {
        pageMap.erase(pageId.pageNumber);
        if (pageMap.size() == 0) {
          this->lockMap.erase(pageId.filename);
        }
      }
    }
    cv.notify_all();
    return true;
  }

  bool getSLock(PageId& pageId, std::time_t& txnTime) {
    return getSLock(pageId, txnTime, defaultWaitTime);
  }

  // return false if it should die...
  bool getSLock(PageId& pageId, std::time_t& txnTime, u32 waitingTime) {
    bool shouldDie = false;

    std::unique_lock<std::mutex> ulock(mut);
    bool succeeded = cv.wait_for(ulock, std::chrono::seconds(waitingTime),
      [this, &shouldDie, &pageId, &txnTime] {
        shouldDie = false;

        auto& lockMap = this->lockMap;
        // if no locks on table
        if (lockMap.find(pageId.filename) == end(lockMap)) {
          return true;
        }

        // if no locks on block
        auto& pageMap = lockMap.at(pageId.filename);
        if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
          return true;
        }

        LockMetaData metadata = pageMap.at(pageId.pageNumber);
        // if is shared lock
        if (metadata.status == SLock) {
          return true;
        }

        // if current txn has lower priority -> needs to die
        if (metadata.earliestTime < txnTime) {
          shouldDie = true;
        }

        return false;
      });

    if (shouldDie || !succeeded) {
      ulock.unlock();
      cv.notify_all();
      std::this_thread::yield();
      return false;
    }
    else {
      if (lockMap.find(pageId.filename) == end(lockMap)) {
        lockMap.insert({ pageId.filename, {} });
      }

      auto& pageMap = lockMap.at(pageId.filename);
      if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
        pageMap.insert({ pageId.pageNumber,  LockMetaData{SLock, 1, txnTime} });
        return true;
      }

      LockMetaData metadata = pageMap.at(pageId.pageNumber);
      metadata.count += 1;
      if (metadata.earliestTime > txnTime) {
        metadata.earliestTime = txnTime;
      }
      pageMap[pageId.pageNumber] = metadata;

      return true;
    }
  }


  // release Xclusive lock
  bool releaseXLock(PageId& pageId) {
    {
      std::lock_guard<std::mutex> lk(mut);

      auto& pageMap = this->lockMap.at(pageId.filename);
      pageMap.erase(pageId.pageNumber);
      if (pageMap.size() == 0) {
        this->lockMap.erase(pageId.filename);
      }
    }
    cv.notify_all();
    return true;
  }

  bool getXLock(PageId& pageId, std::time_t& txnTime) {
    return getXLock(pageId, txnTime, defaultWaitTime);
  }

  bool getXLock(PageId& pageId, std::time_t& txnTime, u32 waitingTime, bool hasSLock = false) {
    bool shouldDie = false;

    std::unique_lock<std::mutex> ulock(mut);
    bool succeeded = cv.wait_for(ulock, std::chrono::seconds(waitingTime),
      [this, &shouldDie, &pageId, &txnTime, hasSLock] {
        // if no locks on table
        if (lockMap.find(pageId.filename) == end(lockMap)) {
          return true;
        }

        // if no locks on block
        auto& pageMap = lockMap.at(pageId.filename);
        if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
          return true;
        }

        // if current caller has SLock and there is only one SLock
        // means the caller can upgrade SLock to XLock
        if (hasSLock) {
          LockMetaData metadata = pageMap.at(pageId.pageNumber);
          if (metadata.count == 1 && metadata.status == SLock) {
            return true;
          }
        }

        // if current txn has lower priority -> needs to die
        LockMetaData metadata = pageMap.at(pageId.pageNumber);
        if (metadata.earliestTime < txnTime) {
          shouldDie = true;
        }

        return false;
      });

    if (shouldDie || !succeeded) {
      ulock.unlock();
      cv.notify_all();
      std::this_thread::yield();
      return false;
    }
    else {
      if (lockMap.find(pageId.filename) == end(lockMap)) {
        lockMap.insert({ pageId.filename, {} });
      }

      auto& pageMap = lockMap.at(pageId.filename);
      if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
        pageMap.insert({ pageId.pageNumber,  LockMetaData{XLock, 1, txnTime} });
      }
      else {
        LockMetaData& metadata = pageMap.at(pageId.pageNumber);
        if (hasSLock && metadata.count == 1 && metadata.status == SLock) {
          metadata.status = XLock;
        }
        else {
          throw std::runtime_error("Not supposed to try to get xlock when block is occupied and is not SLock, programmer error!!!");
        }
      }

      return true;
    }
  }
};
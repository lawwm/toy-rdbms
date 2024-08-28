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
*/

enum LockStatus {
	SLock,
	XLock
};

struct LockMetaData {
	LockStatus status;
	std::time_t earliestTime;
};

class LockManager {
private:
	std::unordered_map<std::string, std::unordered_map<u64, LockMetaData>> lockMap;
	std::mutex mut;
	std::condition_variable cv;
public:

	// return false if it should die...
	bool getSLock(PageId& pageId, std::time_t& txnTime) {
		bool shouldDie = false;

		std::unique_lock<std::mutex> ulock(mut);
		cv.wait(ulock, [&lockMap, &shouldDie] {
			// if no locks on table
			if (lockMap.find(pageId.filename) == end(lockMap)) {
				return true;
			}
			auto& pageMap = lockMap.at(pageId.filename);
			// if no locks on block
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
				return true;
			}

			return false;
		});

		if (shouldDie) {
			return false;
		}

		if (lockMap.find(pageId.filename) == end(lockMap)) {
			lockMap.insert({ pageId.filename, {} });
		}

		auto& pageMap = lockMap.at(pageId.filename);
		if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
			pageMap.insert({pageId.pageNumber,  LockMetaData{SLock, txnTime} });
			return true;
		}

		LockMetaData metadata = pageMap.at(pageId.pageNumber);
		if (metadata.earliestTime > txnTime) {
			pageMap.insert(pageId.pageNumber, LockMetaData{SLock, txnTime});
		}

		return true;
	}

	bool getXLock(PageId& pageId, std::time_t& txnTime) {
		bool shouldDie = false;

		std::unique_lock<std::mutex> ulock(mut);
		cv.wait(ulock, [&lockMap, &shouldDie] {
			// if no locks on table
			if (lockMap.find(pageId.filename) == end(lockMap)) {
				return true;
			}
			
			// if no locks on block
			auto& pageMap = lockMap.at(pageId.filename);
			if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
				return true;
			}

			// if current txn has lower priority -> needs to die
			LockMetaData metadata = pageMap.at(pageId.pageNumber);
			if (metadata.earliestTime < txnTime) {
				shouldDie = true;
				return true;
			}

			return false;
		});

		if (shouldDie) {
			return false;
		}

		if (lockMap.find(pageId.filename) == end(lockMap)) {
			lockMap.insert({ pageId.filename, {} });
		}

		auto& pageMap = lockMap.at(pageId.filename);
		if (pageMap.find(pageId.pageNumber) == end(pageMap)) {
			pageMap.insert({ pageId.pageNumber,  LockMetaData{XLock, txnTime} });
		}
		else {
			throw std::runtime_error("Not supposed to try to get xlock when block is occupied");
		}

		return true;
	}
};
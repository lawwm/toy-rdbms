#include "catch.hpp"

#include "../src/manager/LockManager.h"
#include <vector>
#include <memory>
#include <barrier>

std::string filename = "FileA";
PageId blockA{ filename, 0 };
PageId blockB{ filename, 1 };

u32 aVar = 0, bVar = 0;
std::time_t timeA = 10, timeB = 20, timeC = 30, timeD = 40, timeE = 50;

class MockTransaction {
	std::vector<PageId> pageIdVector;
	std::vector<bool> isSLock;
	std::shared_ptr<LockManager> lm;
public:
	MockTransaction(std::shared_ptr<LockManager> lm) : lm{ lm } {}
	bool getSLock(PageId& pageId, std::time_t& txnTime) {
		if (lm->getSLock(pageId, txnTime)) {
			pageIdVector.push_back(pageId);
			isSLock.push_back(true);
			return true;
		}
		return false;
	}
	bool getXLock(PageId& pageId, std::time_t& txnTime) {
		if (lm->getXLock(pageId, txnTime)) {
			pageIdVector.push_back(pageId);
			isSLock.push_back(false);
			return true;
		}
		return false;
	}

	~MockTransaction() {
		for (int i = 0; i < isSLock.size(); ++i) {
			if (isSLock[i]) {
				lm->releaseSLock(pageIdVector[i]);
			}
			else {
				lm->releaseXLock(pageIdVector[i]);
			}
		}
	}
};

TEST_CASE("Multiple Slock should work") {
	LockManager lm{1};

	// multiple calls to get slock should work.
	REQUIRE(lm.getSLock(blockA, timeA) == true);
	REQUIRE(lm.getSLock(blockA, timeB) == true);
	REQUIRE(lm.getSLock(blockA, timeC) == true);
}

TEST_CASE("Xlock after an SLock should fail if txn time is larger") {
	LockManager lm{1};

	REQUIRE(lm.getSLock(blockA, timeA) == true);
	REQUIRE(lm.getXLock(blockA, timeB) == false);
}

TEST_CASE("Slock after an XLock should fail if txn time is larger") {
	LockManager lm{1};

	REQUIRE(lm.getXLock(blockA, timeA) == true);
	REQUIRE(lm.getSLock(blockA, timeB) == false);
}

TEST_CASE("Xlock after an XLock should fail if txn time is larger") {
	LockManager lm{1};

	REQUIRE(lm.getXLock(blockA, timeB) == true);
	REQUIRE(lm.getXLock(blockA, timeA) == false);
	REQUIRE(lm.getXLock(blockA, timeC) == false);
}

TEST_CASE("Dead Lock should resolve") {
	std::shared_ptr<LockManager> lm = std::make_shared<LockManager>();

	// Initialize a barrier for num_threads threads with a completion function
	std::barrier sync_point(2, []() noexcept {
		std::cout << "All threads have reached the barrier. Proceeding to the next phase...\n";
		});

	std::thread threadA([&lm, &sync_point]() {
		while (true) {
			MockTransaction txn{ lm };
			if (!txn.getXLock(blockA, timeA)) {
				continue;
			}

			sync_point.arrive_and_wait();
			
			if (!txn.getXLock(blockB, timeC)) {
				continue;
			}
			
			// if reach here, end it.
			break;
		}
	});

	std::thread threadB([&lm, &sync_point]() {
		while (true) {
			MockTransaction txn{ lm };
			if (!txn.getXLock(blockB, timeB)) {
				continue;
			}

			sync_point.arrive_and_wait();

			if (!txn.getXLock(blockA, timeD)) {
				continue;
			}

			// if reach here, end it.
			break;
		}
		});

	threadA.join();
	threadB.join();
}
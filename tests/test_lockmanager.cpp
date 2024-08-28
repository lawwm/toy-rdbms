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
	std::time_t txnTime;
public:
	MockTransaction(std::shared_ptr<LockManager> lm, std::time_t txtime) 
		: lm{ lm }, txnTime{ txtime } {}

	bool getSLock(PageId& pageId) {
		if (lm->getSLock(pageId, txnTime)) {
			pageIdVector.push_back(pageId);
			isSLock.push_back(true);
			return true;
		}
		return false;
	}
	bool getXLock(PageId& pageId) {
		if (lm->getXLock(pageId, txnTime)) {
			pageIdVector.push_back(pageId);
			isSLock.push_back(false);
			return true;
		}
		return false;
	}

	~MockTransaction() {
		std::cout << "Restarting Thread" << std::endl;
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
	std::shared_ptr<LockManager> lm = std::make_shared<LockManager>(1);

	// Initialize a barrier for num_threads threads with a completion function
	std::barrier sync_point(2, []() noexcept {
		std::cout << "All threads have reached the barrier. Proceeding to the next phase...\n";
		});

	std::thread threadA([&lm, &sync_point]() {
		u32 count = 0;
		while (true) {
			MockTransaction txn{ lm, timeA };

			std::cout << "Thread A trying to get X Lock for Block A" << std::endl;
			if (!txn.getXLock(blockA)) {
				std::cout << "Thread A can't get X Lock for Block A" << std::endl;
				continue;
			}

			if (count++ == 0) {
				std::cout << "Wait for sync" << std::endl;
				sync_point.arrive_and_wait();
			}
					
			std::cout << "Thread A trying to get X Lock for Block B" << std::endl;
			if (!txn.getXLock(blockB)) {
				std::cout << "Thread A can't get X Lock for Block B" << std::endl;
				continue;
			}
			
			// if reach here, end it.
			break;
		}
	});

	std::thread threadB([&lm, &sync_point]() {
		u32 count = 0;
		while (true) {
			MockTransaction txn{ lm, timeB };

			std::cout << "Thread B trying to get X Lock for Block B" << std::endl;
			if (!txn.getXLock(blockB)) {
				std::cout << "Thread B can't get X Lock for Block B" << std::endl;
				continue;
			}

			if (count++ == 0) {
				std::cout << "Wait for sync" << std::endl;
				sync_point.arrive_and_wait();
			}

			std::cout << "Thread B trying to get X Lock for Block A" << std::endl;
			if (!txn.getXLock(blockA)) {
				std::cout << "Thread B can't get X Lock for Block A" << std::endl;
				continue;
			}

			// if reach here, end it.
			break;
		}
		});

	threadA.join();
	threadB.join();
}
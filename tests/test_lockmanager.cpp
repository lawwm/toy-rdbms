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
  std::string txnName;
  std::time_t txnTime;
  u32 secs;
public:
  MockTransaction(std::string txnName, std::shared_ptr<LockManager> lm, std::time_t txtime, u32 seconds)
    : txnName{ txnName }, lm{ lm }, txnTime{ txtime }, secs{ seconds } {}

  bool getSLock(PageId& pageId) {
    getLock(pageId, false, true);
    if (lm->getSLock(pageId, txnTime, secs)) {
      pageIdVector.push_back(pageId);
      isSLock.push_back(true);
      return true;
    }
    getLock(pageId, true, true);
    return false;
  }

  void getLock(PageId& pageId, bool isFailed, bool isSLock) {
    std::string lock = "X Lock";
    std::string status = "trying";
    if (isSLock) {
      lock = "S Lock";
    }
    if (isFailed) {
      status = "failed";
    }

    std::cout << "Transaction " << txnName <<
      " " << status << " to get " << lock << " for file name " << pageId.filename
      << " and page number " << pageId.pageNumber << std::endl;
  }

  bool getXLock(PageId& pageId) {
    getLock(pageId, false, false);
    if (lm->getXLock(pageId, txnTime, secs)) {
      pageIdVector.push_back(pageId);
      isSLock.push_back(false);
      return true;
    }
    getLock(pageId, true, false);
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
  LockManager lm{ 1 };

  // multiple calls to get slock should work.
  REQUIRE(lm.getSLock(blockA, timeA) == true);
  REQUIRE(lm.getSLock(blockA, timeB) == true);
  REQUIRE(lm.getSLock(blockA, timeC) == true);
}

TEST_CASE("Xlock after an SLock should fail if txn time is larger") {
  LockManager lm{ 1 };

  REQUIRE(lm.getSLock(blockA, timeA) == true);
  REQUIRE(lm.getXLock(blockA, timeB) == false);
}

TEST_CASE("Slock after an XLock should fail if txn time is larger") {
  LockManager lm{ 1 };

  REQUIRE(lm.getXLock(blockA, timeA) == true);
  REQUIRE(lm.getSLock(blockA, timeB) == false);
}

TEST_CASE("Xlock after an XLock should fail if txn time is larger") {
  LockManager lm{ 1 };

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
  std::shared_ptr<std::atomic<int>> counter = std::make_shared<std::atomic<int>>(0);

  std::thread threadA([&lm, &sync_point](std::shared_ptr<std::atomic<int>> counter) {
    u32 count = 0;
    while (true) {
      MockTransaction txn{ "A", lm, timeA, 3 };

      if (!txn.getXLock(blockA)) {
        std::cout << "Restarting Thread for transaction A " << std::endl;
        continue;
      }

      if (count++ == 0) {
        std::cout << "Wait for sync" << std::endl;
        sync_point.arrive_and_wait();
      }

      if (!txn.getXLock(blockB)) {
        std::cout << "Restarting Thread for transaction A " << std::endl;
        continue;
      }

      // if reach here, end it.
      *counter += 1;
      break;
    }
    }, counter);

  // FIgure out why 3 restarts for txn
  // There should only be 1
  std::thread threadB([&lm, &sync_point](std::shared_ptr<std::atomic<int>> counter) {
    u32 count = 0;
    while (true) {
      MockTransaction txn{ "B", lm, timeB, 1 };

      if (!txn.getXLock(blockB)) {
        std::cout << "Restarting Thread for transaction B" << std::endl;
        continue;
      }

      if (count++ == 0) {
        std::cout << "Wait for sync" << std::endl;
        sync_point.arrive_and_wait();
      }

      if (!txn.getXLock(blockA)) {
        std::cout << "Restarting Thread for transaction B" << std::endl;
        continue;
      }

      // if reach here, end it.
      *counter += 1;
      break;
    }
    }, counter);

  threadA.join();
  threadB.join();
  REQUIRE(*counter == 2);

}
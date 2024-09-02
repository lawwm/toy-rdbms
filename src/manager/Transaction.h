#pragma once

#include "./ResourceManager.h"
#include <memory>

class Transaction {
private:
  std::shared_ptr<ResourceManager> rm;
public:
  void commit() {

  }
  void rollback() {

  }
  void recover() {

  }
  BufferFrame* pin(PageId& pageId) {
    return rm->bm.pin(rm->fm, pageId);
  }
  bool unpin(PageId& pageId) {
    return rm->bm.unpin(rm->fm, pageId);
  }
};
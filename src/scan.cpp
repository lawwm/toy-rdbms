
#include "scan.h"

bool TableScan::findNextPage() {
  rm->bm.unpin(rm->fm, this->currentPageId);
  this->currentPageId.pageNumber += 1;

  while (true) {
    this->currBuffer = rm->bm.pin(rm->fm, this->currentPageId);
    if (!this->currBuffer) break;

    this->currentSlot = -1;
    PageType* pt = (PageType*)currBuffer->bufferData.data();
    if (*pt == PageType::TuplePage) {
      return true;
    }
    else {
      rm->bm.unpin(rm->fm, this->currentPageId);
      this->currentPageId.pageNumber += 1;
    }
  }

  return false;
}

bool TableScan::getFirst() {
  currentPageId = PageId{ currentPageId.filename, 0 };
  currBuffer = rm->bm.pin(rm->fm, currentPageId);
  return true;
}

bool TableScan::next() {
  while (true) {
    TuplePage* pe = reinterpret_cast<TuplePage*>(currBuffer->bufferData.data());
    if (pe->pageType != PageType::TuplePage) {
      bool foundNextPage = findNextPage();
      if (!foundNextPage) {
        return false;
      }
      else {
        continue;
      }
    }
    u32 numberOfSlots = pe->numberOfSlots;
    i32 nextSlot = currentSlot + 1;
    if (nextSlot < numberOfSlots) {
      currentSlot = nextSlot;
      return true;
    }
    else { // no more slots left
      bool foundNextPage = findNextPage();
      if (!foundNextPage) {
        return false;
      }

    }
  }
}

Tuple TableScan::get() {
  Slot* slot = reinterpret_cast<Slot*>(currBuffer->bufferData.data() + sizeof(TuplePage));

  std::vector<std::unique_ptr<WriteField>> output;
  u32 offset = slot[currentSlot].getOffset();
  for (int i = 0; i < schema.fieldList.size(); ++i) {
    auto wf = schema.fieldMap[schema.fieldList[i]]->get(currBuffer->bufferData.data(), offset);
    offset += wf->getLength();
    output.push_back(std::move(wf));
  }

  Tuple tuple(std::move(output));
  return tuple;
}

bool SelectScan::getFirst()
{
  return this->scan->getFirst();
}

bool SelectScan::next()
{
  while (this->scan->next()) {
    auto tuple = this->scan->get();
    if (this->predicate->evaluate(tuple, this->schema)) {
      return true;
    }
  }
  return false;
}

Tuple SelectScan::get()
{
  return this->scan->get();
}

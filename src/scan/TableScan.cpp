
#include "TableScan.h"

/**

Read Table Scan

*/

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
  if (currBuffer && currentPageId.pageNumber != 0) {
    rm->bm.unpin(rm->fm, currentPageId);
  }
  currentPageId = PageId{ currentPageId.filename, 0 };
  currBuffer = rm->bm.pin(rm->fm, currentPageId);
  return true;
}

bool TableScan::next() {
  if (!currBuffer) {
    return false;
  }
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

    Slot* slot = reinterpret_cast<Slot*>(currBuffer->bufferData.data() + sizeof(TuplePage));
    i32 nextSlot = currentSlot + 1;
    while (nextSlot < numberOfSlots && !slot[nextSlot].isOccupied()) {
      nextSlot += 1;
    }

    if (nextSlot < numberOfSlots) {
      currentSlot = nextSlot;
      return true;
    }
    else {
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

Schema& TableScan::getSchema()
{
  return this->schema;
}

/*
* ModifyTableScan Implementation
*/

bool ModifyTableScan::getFirst()
{
  return this->iter.findFirstDir();
}

bool ModifyTableScan::next()
{
  bool hasPageBuffer = this->iter.getPageBuffer() != nullptr;
  bool nextSlotIsInSamePageBuffer = false;
  u32 nextSlot = this->currentSlot + 1;
  if (hasPageBuffer) {
    TuplePage* pe = reinterpret_cast<TuplePage*>(this->iter.getPageBuffer()->bufferData.data());
    Slot* slot = reinterpret_cast<Slot*>(this->iter.getPageBuffer()->bufferData.data() + sizeof(TuplePage));
    while (nextSlot < pe->numberOfSlots && !slot[nextSlot].isOccupied()) {
      nextSlot += 1;
    }
    this->currentSlot = nextSlot;
    nextSlotIsInSamePageBuffer = nextSlot < pe->numberOfSlots;
  }


  if (hasPageBuffer && nextSlotIsInSamePageBuffer) {
    // go to next slot
    this->currentSlot = nextSlot;
    return true;
  }
  else {
    // go to the next page
    bool hasNextPage = this->iter.nextPageInDir();
    this->currentSlot = -1;
    if (hasNextPage) {
      return this->next();
    }
    else {
      // go to the next dir
      bool hasNextDir = this->iter.nextDir();
      if (hasNextDir) {
        return this->next();
      }
      else {
        return false;
      }
    }
  }
}

Tuple ModifyTableScan::get()
{
  auto buffer = this->iter.getPageBuffer();
  Slot* slot = reinterpret_cast<Slot*>(buffer->bufferData.data() + sizeof(TuplePage));

  std::vector<std::unique_ptr<WriteField>> output;
  u32 offset = slot[currentSlot].getOffset();
  for (int i = 0; i < schema.fieldList.size(); ++i) {
    auto wf = schema.fieldMap[schema.fieldList[i]]->get(buffer->bufferData.data(), offset);
    offset += wf->getLength();
    output.push_back(std::move(wf));
  }

  return Tuple(std::move(output));
}

// so fucking cooked.
Schema& ModifyTableScan::getSchema()
{
  return this->schema;
}

bool ModifyTableScan::deleteTuple() // delete tuple at current position
{
  auto buffer = this->iter.getPageBuffer();
  if (!buffer) {
    return false;
  }

  // Increase page entry free space size
  BufferFrame* directoryFrame = iter.getPageDirBuffer();
  PageEntry* pageEntryList = reinterpret_cast<PageEntry*>(directoryFrame->bufferData.data() + sizeof(PageDirectory));
  pageEntryList[iter.getPageEntryIndex()].freeSpace += this->get().recordSize;
  directoryFrame->dirty = true;

  // modify the page 
  auto pageBuffer = this->iter.getPageBuffer();
  TuplePage* pe = reinterpret_cast<TuplePage*>(pageBuffer->bufferData.data());
  Slot* slot = reinterpret_cast<Slot*>(buffer->bufferData.data() + sizeof(TuplePage));
  if (currentSlot >= pe->numberOfSlots) {
    return false;
  }
  if (!slot[currentSlot].isOccupied()) {
    return false;
  }

  slot[currentSlot].setOccupied(false);
  // reduce the size of the page entry. this->iter.getPageDirBuffer();
  pageBuffer->dirty = true;
  return true;
}

/*
  lvalue = rvalue/lvalue
*/
void ModifyTableScan::update(UpdateStmt& updateData)
{
  Schema& schema = this->getSchema();
  auto oldTuple = this->get();
  u32 oldRecordSize = oldTuple.recordSize;

  std::vector<Field> tableFieldList;
  std::vector<Field> fieldList;
  for (int i = 0; i < schema.fieldList.size(); ++i) {
    tableFieldList.push_back(Field(schema.tableList[i], schema.fieldList[i]));
    fieldList.push_back(Field(schema.fieldList[i]));
  }

  for (auto& [k, v] : updateData.setFields) {
    Constant newValue = v->getConstant(oldTuple, schema);

    // convert the constant into writeField
    for (u32 i = 0; i < fieldList.size(); ++i) {
      if (fieldList[i] == k.get() || tableFieldList[i] == k.get()) {
        // HACK: this is a hack, we should not be using so much indirection but i'm fucking lazy.

        oldTuple.set(i, schema.fieldMap.at(schema.fieldList[i])->get(newValue));
      }
    }
  }

  auto pageBuffer = this->iter.getPageBuffer();
  Slot* slot = reinterpret_cast<Slot*>(pageBuffer->bufferData.data() + sizeof(TuplePage));
  u32 currSlotIdx = this->currentSlot;
  if (oldRecordSize < oldTuple.recordSize) {
    // if no more space left set to empty, write to next spot
    slot[currSlotIdx].setOccupied(false);
    pageBuffer->dirty = true;

    // decrease page entry free space in current directory.
    BufferFrame* directoryFrame = iter.getPageDirBuffer();
    PageEntry* pageEntryList = reinterpret_cast<PageEntry*>(directoryFrame->bufferData.data() + sizeof(PageDirectory));
    pageEntryList[iter.getPageEntryIndex()].freeSpace += this->get().recordSize;
    directoryFrame->dirty = true;

    // Insert again
    std::vector<Tuple> insertTuples;
    insertTuples.emplace_back(std::move(oldTuple));
    HeapFile::insertTuples(this->pushIter, insertTuples);

  }
  else {
    // if has space, update the current spot.
    u32 offset = slot[currSlotIdx].getOffset();
    for (auto& field : oldTuple.fields) {
      field->write(this->iter.getPageBuffer()->bufferData.data(), offset);
      offset += field->getLength();
    }
    pageBuffer->dirty = true;
  }

}


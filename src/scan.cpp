
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

Schema& TableScan::getSchema()
{
  return this->schema;
}

bool SelectScan::getFirst()
{
  return this->scan->getFirst();
}

bool SelectScan::next()
{
  while (this->scan->next()) {
    auto tuple = this->scan->get();
    if (this->predicate->evaluate(tuple, this->scan->getSchema())) {
      return true;
    }
  }
  return false;
}

Tuple SelectScan::get()
{
  return this->scan->get();
}

Schema& SelectScan::getSchema()
{
  return this->scan->getSchema();
}

bool ProductScan::getFirst()
{
  return leftScan->getFirst()
    && leftScan->next()
    && rightScan->getFirst();
}

bool ProductScan::next()
{
  if (rightScan->next()) {
    return true;
  }
  else {
    rightScan->getFirst();
    return leftScan->next() && rightScan->next();
  }
}

Tuple ProductScan::get()
{
  auto lhs = leftScan->get();
  auto rhs = rightScan->get();
  std::vector<std::unique_ptr<WriteField>> output;
  for (auto& field : lhs.fields) {
    output.push_back(std::move(field));
  }
  for (auto& field : rhs.fields) {
    output.push_back(std::move(field));
  }

  return Tuple(std::move(output));
}

Schema& ProductScan::getSchema()
{
  return this->schema;
}

bool ProjectScan::getFirst()
{
  return this->scan->getFirst();
}

bool ProjectScan::next()
{
  return this->scan->next();
}

Tuple ProjectScan::get()
{
  auto innerTuple = scan->get();
  std::vector<std::unique_ptr<WriteField>> output;
  for (u32 index : mapToInnerSchema) {
    output.push_back(std::move(innerTuple.fields[index]));
  }
  return Tuple(std::move(output));
}

Schema& ProjectScan::getSchema()
{
  return this->schema;
}

// UpdateableTableScan

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
    TuplePage* pe = reinterpret_cast<TuplePage*>(this->iter.getPageBuffer());
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

  Tuple tuple(std::move(output));
  return tuple;
}
// so fucking cooked.
Schema& ModifyTableScan::getSchema()
{
  return this->schema;
}

bool DeleteTableScan::deleteTuple() // delete tuple at current position
{
  auto buffer = this->iter.getPageBuffer();
  if (!buffer) {
    return false;
  }
  TuplePage* pe = reinterpret_cast<TuplePage*>(this->iter.getPageBuffer());
  Slot* slot = reinterpret_cast<Slot*>(buffer->bufferData.data() + sizeof(TuplePage));
  if (currentSlot >= pe->numberOfSlots) {
    return false;
  }
  if (!slot[currentSlot].isOccupied()) {
    return false;
  }

  slot[currentSlot].setOccupied(false);
  // reduce the size of the page entry. this->iter.getPageDirBuffer();
  return true;
}

/*
lvalue = rvalue/lvalue
*/
void UpdateTableScan::update(UpdateStmt& updateData)
{
  Schema& schema = this->getSchema();
  auto oldTuple = this->get();
  u32 oldRecordSize = oldTuple.recordSize;

  std::vector<Field> schemaFields;
  for (int i = 0; i < schema.fieldList.size(); ++i) {
    schemaFields.push_back(Field(schema.tableList[i], schema.fieldList[i]));
  }

  for (auto& [k, v] : updateData.setFields) {
    Constant newValue = v->getConstant(oldTuple, schema);

    // convert the constant into writeField
    for (u32 i = 0; i < schemaFields.size(); ++i) {
      if (schemaFields[i] == k.get()) {
        // HACK: this is a hack, we should not be using so much indirection but i'm fucking lazy.

        oldTuple.set(i, schema.fieldMap.at(schema.fieldList[i])->get(newValue));
      }
    }
  }

  Slot* slot = reinterpret_cast<Slot*>(this->iter.getPageBuffer()->bufferData.data() + sizeof(TuplePage));
  u32 currSlotIdx = this->currentSlot;
  if (oldRecordSize < oldTuple.recordSize) {
    // if no more space left set to empty, write to next spot
    slot[currSlotIdx].setOccupied(false);

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
  }

}
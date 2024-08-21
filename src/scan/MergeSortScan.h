#pragma once

// Open N buffers.
// Sort each page and write to temp table.
// Do a K-way merge on the temp tables.

#include "./TempTable.h"
#include <queue>
#include <functional>

//class MergeSortScan {
//
//public:
//  MergeSortScan(u32 buffers) {
//
//  }
//};

std::unique_ptr<Scan> createSortedTempTable(size_t pageSize, u32 buffers,
  std::unique_ptr<Scan>& input, OrderComparatorGenerator& generator,
  std::string fileName, std::shared_ptr<ResourceManager> resourceManager) {

  // create N temp files, sort within them.
  auto comparator = generator.generate(input->getSchema());
  std::priority_queue<Tuple, std::vector<Tuple>, OrderComparator> pq(comparator);
  u32 buffer_size = 0;
  u32 fileIndex = 0;

  // previous tuple in the file
  Tuple previousTuple({});
  bool noPreviousTuple = true;

  // file list
  std::vector<std::string> fileList;
  fileList.push_back(fileName + "_" + std::to_string(fileIndex));
  HeapFile::createHeapFile(*resourceManager, fileList.back(), 8);
  HeapFile::HeapFileIterator iter = HeapFile::HeapFileIterator(fileList.back(), resourceManager);

  input->getFirst();
  while (input->next()) {
    Tuple tuple = input->get();
    buffer_size += tuple.recordSize;
    pq.push(std::move(tuple));

    if (buffer_size > pageSize) {
      auto m = pq.top();
      buffer_size -= m.recordSize;
    }
  }


  // do K way merge on the temp files.

  // return a table scan on the final temp file.
}


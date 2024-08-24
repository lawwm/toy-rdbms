#pragma once

// Open N buffers.
// Sort each page and write to temp table.
// Do a K-way merge on the temp tables.

#include "./TempTable.h"
#include <queue>
#include <functional>
#include <set>

void print(std::vector<std::string> fileList, std::shared_ptr<ResourceManager> resourceManager, std::unique_ptr<Scan>& input) {
  // print crap
  for (size_t j = 0; j < fileList.size(); j++)
  {
    std::cout << "File: " << fileList[j] << std::endl;
    auto scan = std::make_unique<TableScan>(fileList[j], resourceManager, input->getSchema());
    scan->getFirst();
    while (scan->next())
    {
      std::cout << scan->get() << std::endl;
    }
    std::cout << std::endl;
  }
}

std::unique_ptr<Scan> createSortedTempTable(size_t pageSize, u32 buffers,
  std::unique_ptr<Scan>& input, OrderComparatorGenerator& generator,
  std::string fileName, std::shared_ptr<ResourceManager> resourceManager)
{

  // create N temp files, sort within them.
  auto comparator = generator.generate(input->getSchema());
  std::multiset<Tuple, OrderComparator> set(comparator);

  u32 fileIndex = 0;

  // previous tuple in the file
  Tuple previousTuple({});
  bool noPreviousTuple = true;

  // file list
  std::vector<std::string> fileList;

  fileList.push_back(fileName + "_" + std::to_string(fileIndex++));
  HeapFile::createHeapFile(*resourceManager, fileList.back(), 8);
  std::unique_ptr<HeapFile::HeapFileIterator> iter = std::make_unique<HeapFile::HeapFileIterator>(fileList.back(), resourceManager);

  u32 buffer_size = 0;
  input->getFirst();
  while (input->next())
  {
    if (buffer_size > pageSize)
    {
      // Remove from set
      if (noPreviousTuple)
      {
        noPreviousTuple = false;

        previousTuple = *set.begin();
        buffer_size -= previousTuple.recordSize;

        HeapFile::insertTuple(*iter, previousTuple);
        set.erase(set.begin());
      }
      else
      {
        auto itr = set.lower_bound(previousTuple);
        if (itr == end(set))
        {
          // write the file
          fileList.push_back(fileName + "_" + std::to_string(fileIndex++));
          HeapFile::createHeapFile(*resourceManager, fileList.back(), 8);
          iter = std::make_unique<HeapFile::HeapFileIterator>(fileList.back(), resourceManager);

          noPreviousTuple = true;
        }
        else
        {
          previousTuple = *itr;
          buffer_size -= previousTuple.recordSize;
          HeapFile::insertTuple(*iter, previousTuple);
          set.erase(itr);
        }
      }
    }
    else
      // Add to set
    {
      Tuple tuple = input->get();
      buffer_size += tuple.recordSize;
      set.insert(std::move(tuple));
    }
  }

  // insert remaining map items
  if (set.size() > 0)
  {
    // do not create new file
    if (noPreviousTuple || comparator(previousTuple, *set.begin()) == true)
    {
      std::vector<Tuple> tuples;
      for (auto& tuple : set)
      {
        tuples.push_back(tuple);
      }
      HeapFile::insertTuples(*iter, tuples);
    }
    else
    {
      // write the file
      fileList.push_back(fileName + "_" + std::to_string(fileIndex++));
      HeapFile::createHeapFile(*resourceManager, fileList.back(), 8);
      iter = std::make_unique<HeapFile::HeapFileIterator>(fileList.back(), resourceManager);
      //iter = HeapFile::HeapFileIterator(fileList.back(), resourceManager);

      std::vector<Tuple> tuples;
      for (auto& tuple : set)
      {
        tuples.push_back(tuple);
      }
      HeapFile::insertTuples(*iter, tuples);
    }
  }


  // do K way merge on the temp files.
  while (fileList.size() > 1)
  {
    std::vector<std::string> newFileList;
    for (size_t i = 0; i < fileList.size(); i += buffers)
    {
      // create the file to write to
      newFileList.push_back(fileName + "_" + std::to_string(fileIndex++));
      HeapFile::createHeapFile(*resourceManager, newFileList.back(), 8);
      iter = std::make_unique<HeapFile::HeapFileIterator>(newFileList.back(), resourceManager);

      // open table scans on the files to merge
      std::vector<std::unique_ptr<Scan>> scans;

      // Create priority queue for the table scans
      using TupleWithIndex = std::tuple<Tuple, size_t>;
      auto cmp = [&comparator](TupleWithIndex& lhs, TupleWithIndex& rhs)
        {
          // reverse the parameters because for some reason the priority queue comparison is reversed???
          return comparator(std::get<0>(rhs), std::get<0>(lhs));
        };

      std::priority_queue<TupleWithIndex, std::vector<TupleWithIndex>, decltype(cmp)> pq(cmp);

      for (size_t j = 0; j < buffers && (i + j < fileList.size()); j++)
      {
        scans.push_back(std::make_unique<TableScan>(fileList[i + j], resourceManager, input->getSchema()));
        scans.back()->getFirst();
        if (scans.back()->next())
        {
          pq.push({ scans.back()->get(), j });
        }
      }

      // merge all table scans
      while (!pq.empty())
      {
        auto [tuple, idx] = pq.top();
        pq.pop();

        // write the tuple to the new file
        HeapFile::insertTuple(*iter, tuple);

        // get the next tuple from the scan
        if (scans[idx]->next())
        {
          pq.push({ scans[idx]->get(), idx });
        }
      }
    }

    fileList = std::move(newFileList);
  }


  // return a table scan on the final temp file.
  return std::make_unique<TableScan>(fileList[0], resourceManager, input->getSchema());
}

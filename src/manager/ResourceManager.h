#pragma once

#include "FileManager.h"
#include "BufferManager.h"

struct ResourceManager {
  FileManager fm;
  BufferManager bm;

  ResourceManager(u32 pagesize, u32 poolsize) : fm{ pagesize }, bm{ pagesize, poolsize } {}
};

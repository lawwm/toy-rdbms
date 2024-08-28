#pragma once

#include "FileManager.h"
#include "BufferManager.h"
#include "LockManager.h"

struct ResourceManager {
  FileManager fm;
  BufferManager bm;
  LockManager lm;
  ResourceManager(u32 pagesize, u32 poolsize) : fm{ pagesize }, bm{ pagesize, poolsize }, lm{} {}
};

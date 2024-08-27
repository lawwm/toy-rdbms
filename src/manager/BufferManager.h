#pragma once


struct BufferFrame {
  size_t bufferSize;
  PageId pageId;
  std::vector<char> bufferData;
  int pin;
  bool dirty;

  BufferFrame(size_t bufferSize) : bufferSize{ bufferSize }, pageId{ emptyPageId }, pin{ 0 }, dirty{ false } {
    bufferData.resize(bufferSize);
  }

  ~BufferFrame() {}

  void modify(const void* data, u64 length, u64 offset) {
    std::memcpy(bufferData.data() + offset, data, length);
    dirty = true;
  }

  void clear() {
    this->pageId = emptyPageId;
  }
};


class BufferManager {
private:
  u32 bufferSize;
  std::vector<BufferFrame> bufferPool;

public:
  // todo LRU, now just find first.
  BufferManager(u32 bufferSize, u32 poolSize) : bufferSize{ bufferSize }, bufferPool(poolSize, BufferFrame(bufferSize)) {

  }

  // if pageId doesn't exist, return nullptr.
  BufferFrame* pin(FileManager& fileManager, PageId pageId) {
    // if already pinned, return the buffer
    // if not pinned, find a new buffer.
    // if all buffers are pinned, return nullptr.
    int existingBufferIndex = -1, unpinnedBufferIndex = -1;
    for (int i = 0; i < bufferPool.size(); ++i) {
      auto& buffer = bufferPool[i];
      if (buffer.pin == 0) {
        unpinnedBufferIndex = i;
      }
      if (buffer.pageId == pageId) {
        existingBufferIndex = i;
      }
    }

    if (existingBufferIndex != -1) {
      bufferPool[existingBufferIndex].pin++;
      return &bufferPool[existingBufferIndex];
    }

    if (unpinnedBufferIndex != -1) {
      if (!fileManager.read(pageId, bufferPool[unpinnedBufferIndex].bufferData)) {
        return nullptr;
      }
      bufferPool[unpinnedBufferIndex].pageId = pageId;
      bufferPool[unpinnedBufferIndex].pin++;
      return &bufferPool[unpinnedBufferIndex];
    }

    // wait here...
    return nullptr;
  }

  bool unpin(FileManager& fileManager, PageId pageId) {
    for (auto& buffer : bufferPool) {
      if (buffer.pageId == pageId) {
        buffer.pin--;
        if (buffer.pin < 0) {
          throw std::runtime_error("Breaks invariant: pin count cannot be negative");
        }
        if (buffer.pin == 0 && buffer.dirty) {
          // write to disk
          buffer.dirty = false;
          // page number doesn't exist
          if (!fileManager.write(pageId, buffer.bufferData)) {
            return false;
          }
        }
        return true;
      }
    }

    // buffer doesn't exist
    return false;
  }
};

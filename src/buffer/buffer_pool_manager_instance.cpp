//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include <iostream>

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);
  local_latches_ = std::vector<std::mutex>(pool_size_);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  global_latch_.lock();
  *page_id = AllocatePage();
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Size() > 0) {
      replacer_->Evict(&frame_id);
      page_table_->Remove(pages_[frame_id].GetPageId());
    } else {
      global_latch_.unlock();
      return nullptr;
    }
  }
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(*page_id, frame_id);

  std::scoped_lock<std::mutex> local_lock(local_latches_[frame_id]);
  global_latch_.unlock();

  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  frame_id_t frame_id;
  std::unique_lock<std::mutex> global_lock(global_latch_);
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    std::scoped_lock<std::mutex> local_lock(local_latches_[frame_id]);
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    pages_[frame_id].pin_count_++;
    return pages_ + frame_id;
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Size() > 0) {
      replacer_->Evict(&frame_id);
      page_table_->Remove(pages_[frame_id].GetPageId());
    } else {
      return nullptr;
    }
  }
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);

  std::scoped_lock<std::mutex> local_lock(local_latches_[frame_id]);
  global_lock.unlock();

  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  frame_id_t frame_id;
  std::unique_lock<std::mutex> global_lock(global_latch_);
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  std::scoped_lock<std::mutex> local_lock(local_latches_[frame_id]);
  global_lock.unlock();

  if (pages_[frame_id].GetPinCount() == 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  if (!pages_[frame_id].is_dirty_) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  std::unique_lock<std::mutex> global_lock(global_latch_);
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  std::scoped_lock<std::mutex> local_lock(local_latches_[frame_id]);
  global_lock.unlock();

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      local_latches_[i].lock();
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
      local_latches_[i].unlock();
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  std::unique_lock<std::mutex> global_lock(global_latch_);
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  std::scoped_lock<std::mutex> local_lock(local_latches_[frame_id]);
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  global_lock.unlock();

  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

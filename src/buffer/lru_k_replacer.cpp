//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  std::scoped_lock lock(latch_);
  history_.resize(num_frames);
  allocated_.resize(num_frames);
  evictable_.resize(num_frames);
}

LRUKReplacer::~LRUKReplacer() {
  std::scoped_lock lock(latch_);
  history_.clear();
  allocated_.clear();
  evictable_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  frame_id_t id = 1e9;
  size_t highest_diff = 0;
  for (size_t i = 0; i < replacer_size_; i++) {
    if (!allocated_[i] || !evictable_[i]) {
      continue;
    }
    if (history_[i].size() == k_) {
      size_t diff = current_timestamp_ - history_[i].back();
      if (diff > highest_diff) {
        highest_diff = diff;
        id = i;
      }
    } else {
      highest_diff = 1e9;
      break;
    }
  }
  if (highest_diff == 1e9) {
    size_t earliest_time = current_timestamp_;
    for (size_t i = 0; i < replacer_size_; i++) {
      if (!allocated_[i] || !evictable_[i] || history_[i].size() == k_) {
        continue;
      }
      if (history_[i].back() < earliest_time) {
        earliest_time = history_[i].back();
        id = i;
      }
    }
  }
  *frame_id = id;
  RemoveInternal(id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (!IsValidFrameId(frame_id)) {
    throw std::invalid_argument("RecordAccess(): frame_id is invalid");
  }
  if (!allocated_[frame_id]) {
    allocated_[frame_id] = true;
    evictable_[frame_id] = false;
  }
  history_[frame_id].push_front(current_timestamp_++);
  if (history_[frame_id].size() > k_) {
    history_[frame_id].pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);
  if (!IsValidFrameId(frame_id)) {
    throw std::invalid_argument("SetEvictable(): frame_id is invalid");
  }
  if (!allocated_[frame_id]) {
    return;
  }
  if (!evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  if (evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  if (!IsValidFrameId(frame_id) || !allocated_[frame_id]) {
    return;
  }
  if (!evictable_[frame_id]) {
    throw std::invalid_argument("Remove(): Frame is not evictable");
  }
  RemoveInternal(frame_id);
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  history_[frame_id].clear();
  evictable_[frame_id] = false;
  allocated_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

auto LRUKReplacer::IsValidFrameId(frame_id_t frame_id) -> bool { return frame_id < static_cast<int>(replacer_size_); }

}  // namespace bustub

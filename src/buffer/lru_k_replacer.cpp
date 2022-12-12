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
  history_.resize(num_frames);
  allocated_.resize(num_frames);
  evictable_.resize(num_frames);
}

LRUKReplacer::~LRUKReplacer() {
  history_.clear();
  allocated_.clear();
  evictable_.clear();
  complete_histories_.clear();
  incomplete_histories_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  frame_id_t id;
  if (!incomplete_histories_.empty()) {
    id = incomplete_histories_.begin()->second;
  } else {
    id = complete_histories_.begin()->second;
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

  if (evictable_[frame_id]) {
    if (history_[frame_id].size() > k_) {
      complete_histories_.erase(history_[frame_id].back());
      history_[frame_id].pop_back();
      complete_histories_[history_[frame_id].back()] = frame_id;
    } else if (history_[frame_id].size() == k_) {
      if (history_[frame_id].size() > 1) {
        incomplete_histories_.erase(history_[frame_id].back());
      }
      complete_histories_[history_[frame_id].back()] = frame_id;
    } else {
      incomplete_histories_[history_[frame_id].back()] = frame_id;
    }
  } else {
    if (history_[frame_id].size() > k_) {
      history_[frame_id].pop_back();
    }
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
    if (history_[frame_id].size() == k_) {
      complete_histories_[history_[frame_id].back()] = frame_id;
    } else {
      incomplete_histories_[history_[frame_id].back()] = frame_id;
    }
  }
  if (evictable_[frame_id] && !set_evictable) {
    curr_size_--;
    if (complete_histories_.find(history_[frame_id].back()) != complete_histories_.end()) {
      complete_histories_.erase(history_[frame_id].back());
    }
    if (incomplete_histories_.find(history_[frame_id].back()) != incomplete_histories_.end()) {
      incomplete_histories_.erase(history_[frame_id].back());
    }
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
  if (complete_histories_.find(history_[frame_id].back()) != complete_histories_.end()) {
    complete_histories_.erase(history_[frame_id].back());
  }
  if (incomplete_histories_.find(history_[frame_id].back()) != incomplete_histories_.end()) {
    incomplete_histories_.erase(history_[frame_id].back());
  }
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

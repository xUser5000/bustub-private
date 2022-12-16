//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock<std::shared_mutex> global_lock(global_latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock<std::shared_mutex> global_lock(global_latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> global_lock(global_latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> global_lock(global_latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::shared_mutex> global_lcok(global_latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  global_latch_.lock_shared();
  bool is_inserted = InsertInternal(key, value, 0);
  global_latch_.unlock_shared();
  if (!is_inserted) {
    global_latch_.lock();
    InsertInternal(key, value, 1);
    global_latch_.unlock();
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value, bool resizable) -> bool {
  size_t index = IndexOf(key);
  if (dir_[index]->Insert(key, value)) {
    return true;
  }
  if (!resizable) {
    return false;
  }
  if (dir_[index]->GetDepth() == GetGlobalDepthInternal()) {
    // increment the global depth
    global_depth_++;
    // Double the size of the directory
    int sz = dir_.size();
    dir_.resize(2 * sz);
    for (int i = 0; i < sz; i++) {
      dir_[i + sz] = dir_[i];
    }
  }
  std::shared_ptr<Bucket> old_bucket = dir_[index];
  int old_depth = old_bucket->GetDepth();
  std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, old_depth);
  int step = (1 << old_depth);
  bool turn = false;
  for (int i = index; i < (1 << GetGlobalDepthInternal()); i += step, turn = !turn) {
    if (!turn) {
      dir_[i] = old_bucket;
    } else {
      dir_[i] = new_bucket;
    }
  }
  turn = false;
  for (int i = index; i >= 0; i -= step, turn = !turn) {
    if (!turn) {
      dir_[i] = old_bucket;
    } else {
      dir_[i] = new_bucket;
    }
  }
  std::list<std::pair<K, V>> items = dir_[index]->GetItems();
  for (auto &p : items) {
    old_bucket->Remove(p.first);
    dir_[IndexOf(p.first)]->Insert(p.first, p.second);
  }
  old_bucket->IncrementDepth();
  new_bucket->IncrementDepth();
  num_buckets_++;
  // Retry Insertion
  return InsertInternal(key, value, resizable);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> local_lock(latch_);
  for (auto &p : list_) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::shared_mutex> local_lock(latch_);
  auto it = list_.begin();
  while (it != list_.end()) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
    it++;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::shared_mutex> local_lock(latch_);
  if (IsFull()) {
    return false;
  }
  for (auto &p : list_) {
    if (p.first == key) {
      p.second = value;
      return true;
    }
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

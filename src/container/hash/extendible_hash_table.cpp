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
  std::scoped_lock<std::mutex> lock(latch_);
  std::cout << "GetGlobalDepth()" << '\n';
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  std::cout << "GetLocalDepth()" << '\n';
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  std::cout << "GetNumBuckets()" << '\n';
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  std::cout << "Find " << key << '\n';
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  std::cout << "Remove " << key << '\n';
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  std::cout << "Insert " << key << '\n';
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  size_t index = IndexOf(key);
  if (dir_[index]->Insert(key, value)) {
    return;
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
  /*
  // increment the depth of the bucket
  dir_[index]->IncrementDepth();
  // Split the Bucket
  int other_index;
  if (index < dir_.size() / 2) {
    other_index = index + dir_.size() / 2;
  } else {
    other_index = index - dir_.size() / 2;
  }
  dir_[other_index] = std::make_shared<Bucket>(bucket_size_, GetLocalDepthInternal(index));
  // Redistribute Keys
  std::list<std::pair<K, V>> l = dir_[index]->GetItems();
  for (auto &p : l) {
    dir_[index]->Remove(p.first);
    dir_[IndexOf(p.first)]->Insert(p.first, p.second);
  }
   */

  num_buckets_++;
  // Retry Insertion
  InsertInternal(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
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

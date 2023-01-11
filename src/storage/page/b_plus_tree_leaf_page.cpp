//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::NodeAt(int index) -> MappingType * { return array_ + index; }

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Insert(const KeyType &key, const ValueType &value,
                                                                  const KeyComparator &comparator) -> bool {
  int index;
  for (index = 0; index < GetSize(); index++) {
    if (comparator(key, KeyAt(index)) == 0) {
      return false;
    }
    if (comparator(key, KeyAt(index)) < 0) {
      break;
    }
  }
  for (int i = GetSize(); i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::LowerBound(const KeyType &key,
                                                                      const KeyComparator &comparator) -> int {
  for (int i = GetSize() - 1; i >= 0; i--) {
    auto *cur_node = NodeAt(i);
    if (comparator(key, cur_node->first) >= 0) {
      return i;
    }
  }
  return -1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Remove(const KeyType &key, const KeyComparator &comparator)
    -> bool {
  int index = LowerBound(key, comparator);
  if (index == -1) {
    return false;
  }
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_id_guard_.lock_shared();
  if (IsEmpty()) {
    root_page_id_guard_.unlock_shared();
    return false;
  }
  std::shared_lock<std::shared_mutex> lock(global_latch_);
  return GetValueInternal(root_page_id_, key, result, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::GetValueInternal(page_id_t page_id, const KeyType &key,
                                                                    std::vector<ValueType> *result,
                                                                    Transaction *transaction) -> bool {
  if (page_id == root_page_id_) { root_page_id_guard_.unlock_shared(); }
  auto *cur_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  if (cur_generic_page->IsLeafPage()) {
    auto *cur_leaf_page = reinterpret_cast<LeafPage *>(cur_generic_page);
    auto node = LowerBoundLeaf(cur_leaf_page, key);
    bool ret;
    if (node != nullptr && comparator_(key, node->first) == 0) {
      result->push_back(node->second);
      ret = true;
    } else {
      ret = false;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    return ret;
  }
  auto cur_internal_page = reinterpret_cast<InternalPage *>(cur_generic_page);
  auto node = LowerBoundInternal(cur_internal_page, key);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return GetValueInternal(node->second, key, result, transaction);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_guard_.lock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto root_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(true);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
  std::scoped_lock<std::shared_mutex> global_lock(global_latch_);
  return InsertInternal(root_page_id_, key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternal(page_id_t page_id, const KeyType &key, const ValueType &value,
                                    Transaction *transaction) -> bool {
  if (page_id == root_page_id_) { root_page_id_guard_.unlock(); }
  auto cur_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  auto cur_leaf_page = reinterpret_cast<LeafPage *>(cur_generic_page);
  auto cur_internal_page = reinterpret_cast<InternalPage *>(cur_generic_page);
  if (cur_generic_page->IsLeafPage()) {
    bool is_inserted = InsertIntoLeaf(cur_leaf_page, key, value);
    if (!is_inserted) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      return false;
    }
  } else {
    auto node = LowerBoundInternal(cur_internal_page, key);
    if (!InsertInternal(node->second, key, value, transaction)) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      return false;
    }
  }

  /* If I don't overflow, do nothing */
  if (!IsOverFlowed(cur_generic_page)) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }

  /* If I overflow, do the following: */

  /*
   * If I'm the root,
   * Create a new root and set it as my parent
   * */
  if (cur_generic_page->GetParentPageId() == INVALID_PAGE_ID) {
    page_id_t new_root_page_id;
    auto new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_root_page_id)->GetData());
    new_root_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->NodeAt(0)->second = cur_generic_page->GetPageId();
    cur_generic_page->SetParentPageId(new_root_page_id);
    root_page_id_ = new_root_page_id;
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(false);
  }
  page_id_t parent_page_id = cur_generic_page->GetParentPageId();
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

  /*
   * Create a new sibling
   * Insert all keys >= cur[mid] into it
   * */
  page_id_t sibling_page_id;
  auto sibling_generic_page =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->NewPage(&sibling_page_id)->GetData());
  if (cur_generic_page->IsLeafPage()) {
    auto sibling_leaf_page = static_cast<LeafPage *>(sibling_generic_page);
    sibling_leaf_page->Init(sibling_page_id, parent_page_id, leaf_max_size_);
    for (int i = cur_leaf_page->GetMinSize(), j = 0; i < cur_leaf_page->GetSize(); i++, j++) {
      sibling_leaf_page->NodeAt(j)->first = cur_leaf_page->NodeAt(i)->first;
      sibling_leaf_page->NodeAt(j)->second = cur_leaf_page->NodeAt(i)->second;
    }
    sibling_leaf_page->SetSize(cur_leaf_page->GetSize() - cur_leaf_page->GetMinSize());
    cur_leaf_page->SetSize(cur_leaf_page->GetMinSize());

    InsertIntoInternal(parent_page, sibling_leaf_page->NodeAt(0)->first, sibling_leaf_page->GetPageId());
    cur_leaf_page->SetNextPageId(sibling_generic_page->GetPageId());
  } else {
    auto sibling_internal_page = static_cast<InternalPage *>(sibling_generic_page);
    sibling_internal_page->Init(sibling_page_id, parent_page_id, internal_max_size_);

    for (int i = cur_internal_page->GetSize() / 2, j = 0; i < cur_internal_page->GetSize(); i++, j++) {
      sibling_internal_page->NodeAt(j)->first = cur_internal_page->NodeAt(i)->first;
      sibling_internal_page->NodeAt(j)->second = cur_internal_page->NodeAt(i)->second;
    }
    sibling_internal_page->SetSize(cur_internal_page->GetSize() - cur_internal_page->GetSize() / 2);
    cur_internal_page->SetSize(cur_internal_page->GetSize() / 2);

    for (int i = 0; i < sibling_internal_page->GetSize(); i++) {
      auto child_generic_page = reinterpret_cast<BPlusTreePage *>(
          buffer_pool_manager_->FetchPage(sibling_internal_page->NodeAt(i)->second)->GetData());
      child_generic_page->SetParentPageId(sibling_internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(child_generic_page->GetPageId(), true);
    }

    InsertIntoInternal(parent_page, sibling_internal_page->NodeAt(0)->first, sibling_internal_page->GetPageId());
  }

  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(sibling_page_id, true);
  return true;
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_guard_.lock();
  if (IsEmpty()) {
    root_page_id_guard_.unlock();
    return;
  }
  std::unique_lock<std::shared_mutex> global_lock(global_latch_);
  RemoveInternal(root_page_id_, key, transaction);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::RemoveInternal(page_id_t page_id, const KeyType &key,
                                                                  Transaction *transaction) {
  if (page_id == root_page_id_) { root_page_id_guard_.unlock(); }
  auto cur_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  auto cur_leaf_page = static_cast<LeafPage *>(cur_generic_page);
  auto cur_internal_page = static_cast<InternalPage *>(cur_generic_page);
  if (cur_generic_page->IsLeafPage()) {
    if (RemoveFromLeaf(cur_leaf_page, key)) {
    } else {
      buffer_pool_manager_->UnpinPage(page_id, false);
      return;
    }
  } else {
    auto node = LowerBoundInternal(cur_internal_page, key);
    RemoveInternal(node->second, key, transaction);
  }

  if (root_page_id_ == page_id || !IsUnderFlowed(cur_generic_page)) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }

  auto parent_page_id = cur_generic_page->GetParentPageId();
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());

  int index = 0;
  for (int i = 0; i < parent_page->GetSize(); i++) {
    if (parent_page->NodeAt(i)->second == page_id) {
      index = i;
      break;
    }
  }

  /* Borrow from my left sibling if possible */
  auto left_sibling_page_id = (index > 0) ? parent_page->NodeAt(index - 1)->second : INVALID_PAGE_ID;
  if (left_sibling_page_id != INVALID_PAGE_ID) {
    auto left_sibling_generic_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_sibling_page_id)->GetData());
    int left_sibling_size = left_sibling_generic_page->GetSize();
    if (left_sibling_generic_page->GetSize() > left_sibling_generic_page->GetMinSize()) {
      if (cur_generic_page->IsLeafPage()) {
        auto left_sibling_leaf_page = static_cast<LeafPage *>(left_sibling_generic_page);
        InsertIntoLeaf(cur_leaf_page, left_sibling_leaf_page->NodeAt(left_sibling_size - 1)->first,
                       left_sibling_leaf_page->NodeAt(left_sibling_size - 1)->second);
        parent_page->NodeAt(index)->first = cur_leaf_page->NodeAt(0)->first;
      } else {
        auto left_sibling_internal_page = static_cast<InternalPage *>(left_sibling_generic_page);
        InsertIntoInternal(cur_internal_page, left_sibling_internal_page->NodeAt(left_sibling_size - 1)->first,
                           left_sibling_internal_page->NodeAt(left_sibling_size - 1)->second);
        parent_page->NodeAt(index)->first = cur_internal_page->NodeAt(0)->first;

        page_id_t child_page_id = cur_internal_page->NodeAt(0)->second;
        auto child_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id));
        child_generic_page->SetPageId(cur_generic_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page_id, true);
      }
      left_sibling_generic_page->SetSize(left_sibling_size - 1);
      buffer_pool_manager_->UnpinPage(page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(left_sibling_page_id, true);
      return;
    }
  }

  /* Borrow from my right sibling if possible */
  auto right_sibling_page_id =
      (index < parent_page->GetSize() - 1) ? parent_page->NodeAt(index + 1)->second : INVALID_PAGE_ID;
  if (right_sibling_page_id != INVALID_PAGE_ID) {
    auto right_sibling_generic_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_sibling_page_id)->GetData());
    int right_sibling_size = right_sibling_generic_page->GetSize();
    if (right_sibling_generic_page->GetSize() > right_sibling_generic_page->GetMinSize()) {
      if (cur_generic_page->IsLeafPage()) {
        auto right_sibling_leaf_page = static_cast<LeafPage *>(right_sibling_generic_page);
        InsertIntoLeaf(cur_leaf_page, right_sibling_leaf_page->NodeAt(0)->first,
                       right_sibling_leaf_page->NodeAt(0)->second);
        RemoveFromLeaf(right_sibling_leaf_page, right_sibling_leaf_page->NodeAt(0)->first);
        parent_page->NodeAt(index + 1)->first = right_sibling_leaf_page->NodeAt(0)->first;
      } else {
        auto right_sibling_internal_page = static_cast<InternalPage *>(right_sibling_generic_page);
        InsertIntoInternal(cur_internal_page, right_sibling_internal_page->NodeAt(0)->first,
                           right_sibling_internal_page->NodeAt(0)->second);
        for (int i = 0; i < right_sibling_size - 1; i++) {
          right_sibling_internal_page->NodeAt(i)->first = right_sibling_internal_page->NodeAt(i + 1)->first;
          right_sibling_internal_page->NodeAt(i)->second = right_sibling_internal_page->NodeAt(i + 1)->second;
        }
        right_sibling_internal_page->SetSize(right_sibling_size - 1);
        parent_page->NodeAt(index + 1)->first = right_sibling_internal_page->NodeAt(0)->first;

        page_id_t child_page_id = cur_internal_page->NodeAt(cur_generic_page->GetSize() - 1)->second;
        auto child_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_page_id));
        child_generic_page->SetPageId(cur_generic_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page_id, true);
      }
      buffer_pool_manager_->UnpinPage(page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(right_sibling_page_id, true);
      return;
    }
  }

  /* If I can't borrow from neither of my siblings: */

  /* If I'm the only child of my parent, return */
  if (parent_page->GetSize() == 1) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }

  /* Merge with (any) one of my siblings */
  BPlusTreePage *left_generic_page;
  BPlusTreePage *right_generic_page;
  if (index < parent_page->GetSize() - 1) {
    left_sibling_page_id = page_id;
    left_generic_page = cur_generic_page;
    right_sibling_page_id = parent_page->NodeAt(index + 1)->second;
    right_generic_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_sibling_page_id)->GetData());
  } else {
    left_sibling_page_id = parent_page->NodeAt(index - 1)->second;
    left_generic_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_sibling_page_id)->GetData());
    right_sibling_page_id = page_id;
    right_generic_page = cur_generic_page;
  }

  if (left_generic_page->IsLeafPage()) {
    auto left_leaf_page = static_cast<LeafPage *>(left_generic_page);
    auto right_leaf_page = static_cast<LeafPage *>(right_generic_page);
    for (int i = 0; i < right_leaf_page->GetSize(); i++) {
      InsertIntoLeaf(left_leaf_page, right_leaf_page->NodeAt(i)->first, right_leaf_page->NodeAt(i)->second);
    }
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
  } else {
    auto left_internal_page = static_cast<InternalPage *>(left_generic_page);
    auto right_internal_page = static_cast<InternalPage *>(right_generic_page);
    for (int i = 0; i < right_internal_page->GetSize(); i++) {
      InsertIntoInternal(left_internal_page, right_internal_page->NodeAt(i)->first,
                         right_internal_page->NodeAt(i)->second);
    }
  }

  for (index = 1; index < parent_page->GetSize(); index++) {
    if (parent_page->NodeAt(index)->second == right_generic_page->GetPageId()) {
      break;
    }
  }
  for (int i = index; i < parent_page->GetSize() - 1; i++) {
    parent_page->NodeAt(i)->first = parent_page->NodeAt(i + 1)->first;
    parent_page->NodeAt(i)->second = parent_page->NodeAt(i + 1)->second;
  }

  buffer_pool_manager_->UnpinPage(left_sibling_page_id, true);
  buffer_pool_manager_->UnpinPage(right_sibling_page_id, true);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->DeletePage(right_sibling_page_id);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::unique_lock<std::shared_mutex> lock(global_latch_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }
  return BeginInternal(root_page_id_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::BeginInternal(page_id_t cur_page_id)
    -> IndexIterator<KeyType, ValueType, KeyComparator> {
  auto cur_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_page_id)->GetData());
  if (cur_generic_page->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    return INDEXITERATOR_TYPE(buffer_pool_manager_, cur_page_id, 0);
  }
  auto cur_internal_page = static_cast<InternalPage *>(cur_generic_page);
  page_id_t left_child_page_id = cur_internal_page->NodeAt(0)->second;
  buffer_pool_manager_->UnpinPage(cur_page_id, false);
  return BeginInternal(left_child_page_id);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::unique_lock<std::shared_mutex> lock(global_latch_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }
  return BeginInternal(root_page_id_, key);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::BeginInternal(page_id_t cur_page_id, const KeyType &key)
    -> IndexIterator<KeyType, ValueType, KeyComparator> {
  auto cur_generic_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_page_id)->GetData());
  if (cur_generic_page->IsLeafPage()) {
    auto cur_leaf_page = static_cast<LeafPage *>(cur_generic_page);
    int cur_leaf_size = cur_leaf_page->GetSize();
    int index = 0;
    for (index = 0; index < cur_leaf_size; index++) {
      if (comparator_(key, cur_leaf_page->NodeAt(index)->first) == 0) {
        break;
      }
    }
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    if (index < cur_leaf_size) {
      return INDEXITERATOR_TYPE(buffer_pool_manager_, cur_page_id, index);
    }
    return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
  }
  auto cur_internal_page = static_cast<InternalPage *>(cur_generic_page);
  page_id_t next_page_id = LowerBoundInternal(cur_internal_page, key)->second;
  buffer_pool_manager_->UnpinPage(cur_page_id, false);
  return BeginInternal(next_page_id, key);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::InsertIntoLeaf(BPlusTree::LeafPage *page, const KeyType &key,
                                                                  const ValueType &value) -> bool {
  /* Find the proper location to insert */
  int index;
  for (index = 0; index < page->GetSize(); index++) {
    /*
     * If the key already exists, return false.
     * We don't want duplicate keys
     * */
    if (comparator_(key, page->KeyAt(index)) == 0) {
      return false;
    }

    if (comparator_(key, page->KeyAt(index)) < 0) {
      break;
    }
  }
  /* Shift all the elements after index one step to the right */
  for (int i = page->GetSize(); i > index; i--) {
    page->NodeAt(i)->first = page->NodeAt(i - 1)->first;
    page->NodeAt(i)->second = page->NodeAt(i - 1)->second;
  }
  /* Put the KV pair into index and increment the size */
  page->NodeAt(index)->first = key;
  page->NodeAt(index)->second = value;
  page->IncreaseSize(1);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::LowerBoundLeaf(BPlusTree::LeafPage *page, const KeyType &key)
    -> std::pair<KeyType, ValueType> * {
  for (int i = page->GetSize() - 1; i >= 0; i--) {
    std::pair<KeyType, ValueType> *cur_node = page->NodeAt(i);
    if (comparator_(key, cur_node->first) >= 0) {
      return cur_node;
    }
  }
  return nullptr;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::InsertIntoInternal(BPlusTree::InternalPage *page, const KeyType &key,
                                                                      page_id_t child_page_id) -> bool {
  int index;
  for (index = page->GetSize() - 1; index >= 1; index--) {
    if (comparator_(key, page->NodeAt(index)->first) >= 0) {
      break;
    }
  }
  for (int i = page->GetSize(); i > index + 1; i--) {
    page->NodeAt(i)->first = page->NodeAt(i - 1)->first;
    page->NodeAt(i)->second = page->NodeAt(i - 1)->second;
  }
  page->NodeAt(index + 1)->first = key;
  page->NodeAt(index + 1)->second = child_page_id;
  page->IncreaseSize(1);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::LowerBoundInternal(BPlusTree::InternalPage *page, const KeyType &key)
    -> std::pair<KeyType, page_id_t> * {
  int index;
  for (index = page->GetSize() - 1; index >= 1; index--) {
    if (comparator_(key, page->NodeAt(index)->first) >= 0) {
      break;
    }
  }
  return page->NodeAt(index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::IsOverFlowed(BPlusTreePage *page) -> bool {
  return (page->IsLeafPage() && page->GetSize() == page->GetMaxSize()) ||
         (!page->IsLeafPage() && page->GetSize() == page->GetMaxSize() + 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::IsUnderFlowed(BPlusTreePage *page) -> bool {
  return page->GetSize() < page->GetMinSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTree<KeyType, ValueType, KeyComparator>::RemoveFromLeaf(BPlusTree::LeafPage *page, const KeyType &key)
    -> bool {
  int index;
  for (index = 0; index < page->GetSize(); index++) {
    if (comparator_(key, page->NodeAt(index)->first) == 0) {
      break;
    }
  }
  if (index == page->GetSize()) {
    return false;
  }
  for (int i = index; i < page->GetSize() - 1; i++) {
    page->NodeAt(i)->first = page->NodeAt(i + 1)->first;
    page->NodeAt(i)->second = page->NodeAt(i + 1)->second;
  }
  page->SetSize(page->GetSize() - 1);
  return true;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

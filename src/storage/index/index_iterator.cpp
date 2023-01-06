/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator>::IndexIterator(BufferPoolManager *buffer_pool_manager,
                                                                page_id_t page_id, int index) {
  buffer_pool_manager_ = buffer_pool_manager;
  page_id_ = page_id;
  index_ = index;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { buffer_pool_manager_->UnpinPage(page_id_, false); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto cur_leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(page_id_)->GetData());
  MappingType &node = *cur_leaf_page->NodeAt(index_);
  buffer_pool_manager_->UnpinPage(page_id_, false);
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  page_id_t old_page_id = page_id_;
  auto cur_leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(old_page_id)->GetData());
  if (index_ + 1 < cur_leaf_page->GetSize()) {
    index_++;
  } else {
    page_id_ = cur_leaf_page->GetNextPageId();
    index_ = 0;
  }
  buffer_pool_manager_->UnpinPage(old_page_id, false);
  return *this;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto IndexIterator<KeyType, ValueType, KeyComparator>::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto IndexIterator<KeyType, ValueType, KeyComparator>::operator!=(const IndexIterator &itr) const -> bool {
  return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

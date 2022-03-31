/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/logger.h"
#include "storage/index/index_iterator.h"
namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *lp, int current_index, BufferPoolManager *bpm)
    : lp_(lp), current_index_(current_index), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { bpm_->UnpinPage(lp_->GetPageId(), false); }

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return lp_->GetSize() == current_index_ && lp_->GetNextPageId() == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return lp_->GetItem(current_index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  current_index_++;
  page_id_t next_page_id = lp_->GetNextPageId();
  if (lp_->GetSize() == current_index_ && next_page_id != INVALID_PAGE_ID) {
    page_id_t old_id = lp_->GetPageId();
    auto page = bpm_->FetchPage(next_page_id);
    lp_ = reinterpret_cast<LeafPage *>(page->GetData());
    current_index_ = 0;
    bpm_->UnpinPage(old_id, false);
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

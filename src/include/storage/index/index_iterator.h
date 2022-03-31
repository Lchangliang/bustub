//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  IndexIterator();
  // you may define your own constructor based on your member variables
  IndexIterator(LeafPage *lp, int current_index, BufferPoolManager *bpm);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return lp_->GetPageId() == itr.lp_->GetPageId() && current_index_ == itr.current_index_;
  }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

 private:
  // add your own private member variables here
  LeafPage *lp_;
  int current_index_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub

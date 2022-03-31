//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
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
bool BPLUSTREE_TYPE::IsEmpty() const {
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
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  auto page = buffer_pool_manager_->FetchPage(guard_page_id);
  page->RLatch();
//  LOG_INFO("RLock %d", guard_page_id);
  if (IsEmpty()) {
//    LOG_INFO("RUnlock %d", guard_page_id);
    page->RUnlatch();
    return false;
  }
  transaction->AddIntoPageSet(page);
  bool get_value = false;

  page = FindLeafPageWithLock(key, OpType::READ, transaction);

  LeafPage *lp = reinterpret_cast<LeafPage *>(page->GetData());
  result->resize(1);
  get_value = lp->Lookup(key, &result->at(0), comparator_);
  if (!get_value) {
    LOG_INFO("%d %s", page->GetPageId(), result->at(0).ToString().c_str());
  }

  ReleaseAndUnpin(OpType::READ, transaction);
  return get_value;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto page = buffer_pool_manager_->FetchPage(guard_page_id);
  page->WLatch();
//  LOG_INFO("Insert WLock %d", guard_page_id);
  if (IsEmpty()) {
    StartNewTree(key, value);
//    LOG_INFO("Insert WUnLock %d", guard_page_id);
    page->WUnlatch();
    return true;
  }
  transaction->AddIntoPageSet(page);
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_page_id;
  auto page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "The Buffer Pool Manager is out of range");
  }
  BPlusTreePage *bptp = reinterpret_cast<LeafPage *>(page->GetData());
  bptp->SetPageId(new_page_id);
  InitBPlusTreePage(IndexPageType::LEAF_PAGE, bptp);
  LeafPage *lp = reinterpret_cast<LeafPage *>(bptp);
  lp->SetNextPageId(INVALID_PAGE_ID);
  lp->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  root_page_id_ = new_page_id;
  UpdateRootPageId(1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InitBPlusTreePage(IndexPageType page_type, BPlusTreePage *bptp, page_id_t parent_page_id) {
  bptp->SetParentPageId(parent_page_id);
  bptp->SetSize(0);
  bptp->SetPageType(page_type);
  if (page_type == IndexPageType::INTERNAL_PAGE) {
    bptp->SetMaxSize(internal_max_size_);
  } else if (page_type == IndexPageType::LEAF_PAGE) {
    bptp->SetMaxSize(leaf_max_size_);
  } else {
    // TODO(liuchangliang): Error
    assert(0);
  }
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPageWithLock(key, OpType::INSERT, transaction);
  if (page->GetPageId() == 0) {
    Draw(buffer_pool_manager_, "mytree.dot");
    BUSTUB_ASSERT(0, "error!");
  }
  LeafPage *lp = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType tmp;
  if (lp->Lookup(key, &tmp, comparator_)) {
    ReleaseAndUnpin(OpType::INSERT, transaction);
    return false;
  }
  lp->Insert(key, value, comparator_);
  if (lp->GetSize() == lp->GetMaxSize()) {
    LeafPage *new_lp = Split<LeafPage>(lp);
    lp->MoveHalfTo(new_lp);
    auto page_id = lp->GetNextPageId();
    new_lp->SetNextPageId(page_id);
    lp->SetNextPageId(new_lp->GetPageId());
    InsertIntoParent(lp, new_lp->KeyAt(0), new_lp, transaction);
    buffer_pool_manager_->UnpinPage(new_lp->GetPageId(), true);
  }
  ReleaseAndUnpin(OpType::INSERT, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id = 0;
  auto page = buffer_pool_manager_->NewPage(&new_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "The Buffer Pool Manager is out of range");
  }
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  bptp->SetPageId(new_page_id);
  IndexPageType type = node->IsLeafPage() ? IndexPageType::LEAF_PAGE : IndexPageType::INTERNAL_PAGE;
  InitBPlusTreePage(type, bptp, node->GetParentPageId());
  return reinterpret_cast<N *>(bptp);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  auto parent_page_id = old_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
    InitBPlusTreePage(IndexPageType::INTERNAL_PAGE, bptp);
    InternalPage *ip = reinterpret_cast<InternalPage *>(bptp);
    ip->SetPageId(root_page_id_);
    ip->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
    return;
  }
  auto page = buffer_pool_manager_->FetchPage(parent_page_id);
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  InternalPage *ip = reinterpret_cast<InternalPage *>(bptp);
  ip->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // 溢出才分裂
  // TODO(liuchangliang): 会溢出
  if (ip->GetSize() > ip->GetMaxSize()) {
    InternalPage *new_ip = reinterpret_cast<InternalPage*>(Split<BPlusTreePage>(ip));
    ip->MoveHalfTo(new_ip, buffer_pool_manager_);
    KeyType new_key = new_ip->KeyAt(0);
    InsertIntoParent(ip, new_key, new_ip, transaction);
    buffer_pool_manager_->UnpinPage(new_ip->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  auto page = buffer_pool_manager_->FetchPage(guard_page_id);
  page->WLatch();
//  LOG_INFO("Remove WLock %d", guard_page_id);
  if (IsEmpty()) {
//    LOG_INFO("Remove WULock %d", guard_page_id);
    page->WUnlatch();
    return;
  }
  transaction->AddIntoPageSet(page);
  page = FindLeafPageWithLock(key, OpType::DELETE, transaction);
  LeafPage *lp = reinterpret_cast<LeafPage *>(page->GetData());
  int old_size = lp->GetSize();
  int new_size = lp->RemoveAndDeleteRecord(key, comparator_);
  if (old_size == new_size) { // 没有存在该Key
    ReleaseAndUnpin(OpType::DELETE, transaction);
    return ;
  }
  if (lp->GetSize() < lp->GetMinSize()) {
    CoalesceOrRedistribute<BPlusTreePage>(lp, transaction);
  }
  ReleaseAndUnpin(OpType::DELETE, transaction);
}


/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  BPlusTreePage* bptp = reinterpret_cast<BPlusTreePage*>(node);
  if (bptp->IsRootPage()) {
    bool flag =  AdjustRoot(bptp);
    if (flag) {
      transaction->AddIntoDeletedPageSet(bptp->GetPageId());
    }
    return flag;
  }
  page_id_t parent_id = bptp->GetParentPageId();
  BPlusTreePage* parent_bptp = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  InternalPage *parent_ip = reinterpret_cast<InternalPage *>(parent_bptp);
  int index = parent_ip->ValueIndex(bptp->GetPageId());
  int left_sib_index = index - 1;
  if (left_sib_index >= 0) {
    page_id_t left_sib_id = parent_ip->ValueAt(left_sib_index);
    auto page = buffer_pool_manager_->FetchPage(left_sib_id);
    page->WLatch();
//    LOG_INFO("Left Coalesce Lock %d", page->GetPageId());
    N* sib_node = reinterpret_cast<N*>(page->GetData());
    if (IsCoalesce(node, sib_node)) {
//      LOG_INFO("%d Coalesce %d, parentID %d", node->GetPageId(), sib_node->GetPageId() , parent_id);
      Coalesce(&sib_node, &node, &parent_ip, index, transaction);
//      LOG_INFO("Left Coalesce Unlock %d", page->GetPageId());
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sib_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    } else {
//      LOG_INFO("Left Coalesce Unlock %d", page->GetPageId());
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_sib_id, false);
    }
  }
  int right_sib_index = index + 1;
  if (right_sib_index < parent_ip->GetSize()) {
//    LOG_INFO("right_sib_index %d", right_sib_index);
    page_id_t right_sib_id = parent_ip->ValueAt(right_sib_index);
    auto page = buffer_pool_manager_->FetchPage(right_sib_id);
    page->WLatch();
//    LOG_INFO("Right Coalesce Lock %d", page->GetPageId());
    N* sib_node = reinterpret_cast<N*>(page->GetData());
    if (IsCoalesce(node, sib_node)) {
      Coalesce(&node, &sib_node, &parent_ip, right_sib_index, transaction);
//      LOG_INFO("Right Coalesce RUnlock %d", page->GetPageId());
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sib_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    } else {
//      LOG_INFO("Right Coalesce RUnlock %d", page->GetPageId());
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_sib_id, false);
    }
  }

  if (left_sib_index >= 0) {
    page_id_t left_sib_id = parent_ip->ValueAt(left_sib_index);
    auto page = buffer_pool_manager_->FetchPage(left_sib_id);
    page->WLatch();
//    LOG_INFO("Left Redistribute Lock %d", page->GetPageId());
    N* sib_node = reinterpret_cast<N*>(page->GetData());
    if (node->IsLeafPage()) {
      LeafPage* sib_lp = reinterpret_cast<LeafPage*>(sib_node);
      auto key = sib_lp->KeyAt(sib_lp->GetSize()-1);
      parent_ip->SetKeyAt(index, key);
      Redistribute(sib_node, node, 1);
    } else {
      InternalPage* cur_ip = reinterpret_cast<InternalPage*>(node);
      InternalPage* sib_ip = reinterpret_cast<InternalPage*>(sib_node);
      auto middle_key = parent_ip->KeyAt(index);
      cur_ip->SetKeyAt(0, middle_key);
      parent_ip->SetKeyAt(index, sib_ip->KeyAt(sib_ip->GetSize()-1));
      Redistribute(sib_node, node, 1);
    }
//    LOG_INFO("Left Redistribute UnLock %d", page->GetPageId());
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_sib_id, true);
  } else {
    page_id_t right_sib_id = parent_ip->ValueAt(right_sib_index);
    auto page = buffer_pool_manager_->FetchPage(right_sib_id);
    page->WLatch();
//    LOG_INFO("Right Redistribute Lock %d", page->GetPageId());
    N* sib_node = reinterpret_cast<N*>(page->GetData());
//    LOG_INFO("%d Redistribute %d, ParentID %d", node->GetPageId(), sib_node->GetPageId(), parent_id);
    if (node->IsLeafPage()) {
      LeafPage* sib_lp = reinterpret_cast<LeafPage*>(sib_node);
      auto key = sib_lp->KeyAt(1);
//      LOG_INFO("middle key %lld", key.ToString());
      parent_ip->SetKeyAt(right_sib_index, key);
      Redistribute(sib_node, node, 0);
    } else {
      InternalPage* sib_ip = reinterpret_cast<InternalPage*>(sib_node);
      auto middle_key = parent_ip->KeyAt(right_sib_index);
      sib_ip->SetKeyAt(0, middle_key);
      parent_ip->SetKeyAt(right_sib_index, sib_ip->KeyAt(1));
      Redistribute(sib_node, node, 0);
    }
//    LOG_INFO("Right Redistribute UnLock %d", page->GetPageId());
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_sib_index, true);
  }
  buffer_pool_manager_->UnpinPage(parent_id, true);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
//  LOG_INFO("Coalesce Size %d, %d", (*neighbor_node)->GetSize(), (*node)->GetSize());
  if ((*node)->IsLeafPage()) {
    LeafPage* cur_lp = reinterpret_cast<LeafPage*>((*node));
    LeafPage* sib_lp = reinterpret_cast<LeafPage*>((*neighbor_node));
    cur_lp->MoveAllTo(sib_lp);
    sib_lp->SetNextPageId(cur_lp->GetNextPageId());
  } else {
    InternalPage* cur_ip = reinterpret_cast<InternalPage*>((*node));
    InternalPage* sib_ip = reinterpret_cast<InternalPage*>((*neighbor_node));
    cur_ip->MoveAllTo(sib_ip, (*parent)->KeyAt(index), buffer_pool_manager_);
  }

  transaction->AddIntoDeletedPageSet((*node)->GetPageId());

  (*parent)->Remove(index);

  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    bool flag = CoalesceOrRedistribute<BPlusTreePage>(*parent, transaction);
    return flag;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsCoalesce(N* lhs, N* rhs) {
  return lhs->GetSize() + rhs->GetSize() < lhs->GetMaxSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    // right -> left
    if (node->IsLeafPage()) {
      LeafPage* cur_lp = reinterpret_cast<LeafPage*>(node);
      LeafPage* sib_lp = reinterpret_cast<LeafPage*>(neighbor_node);
      sib_lp->MoveFirstToEndOf(cur_lp);
    } else {
      InternalPage* cur_ip = reinterpret_cast<InternalPage*>(node);
      InternalPage* sib_ip = reinterpret_cast<InternalPage*>(neighbor_node);
      sib_ip->MoveFirstToEndOf(cur_ip, sib_ip->KeyAt(0), buffer_pool_manager_);
    }
  } else {
    // left -> right
    if (node->IsLeafPage()) {
      LeafPage* cur_lp = reinterpret_cast<LeafPage*>(node);
      LeafPage* sib_lp = reinterpret_cast<LeafPage*>(neighbor_node);
      sib_lp->MoveLastToFrontOf(cur_lp);
    } else {
      InternalPage* cur_ip = reinterpret_cast<InternalPage*>(node);
      InternalPage* sib_ip = reinterpret_cast<InternalPage*>(neighbor_node);
      sib_ip->MoveLastToFrontOf(cur_ip, cur_ip->KeyAt(0), buffer_pool_manager_);
    }
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  bool flag = true;
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
  } else if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto ip = reinterpret_cast<InternalPage*>(old_root_node);
    root_page_id_ = ip->RemoveAndReturnOnlyChild();
//    LOG_INFO("New root_page_id %d", root_page_id_);
    auto root_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    root_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    flag = false;
  }
  if (flag) {
    UpdateRootPageId();
  }
  return flag;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key;
  return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(FindLeafPage(key, true)->GetData()), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LeafPage *lp = reinterpret_cast<LeafPage *>(FindLeafPage(key)->GetData());
  return INDEXITERATOR_TYPE(lp, lp->KeyIndex(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  page_id_t page_id = root_page_id_;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!bptp->IsLeafPage()) {
    auto ip = reinterpret_cast<InternalPage *>(bptp);
    page_id_t next_page_id = ip->ValueAt(ip->GetSize() - 1);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    page_id = next_page_id;
    bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  LeafPage *lp = reinterpret_cast<LeafPage *>(bptp);
  return INDEXITERATOR_TYPE(lp, lp->GetSize(), buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  page_id_t page_id = root_page_id_;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!bptp->IsLeafPage()) {
    auto ip = reinterpret_cast<InternalPage *>(bptp);
    page_id_t next_page_id = INVALID_PAGE_ID;
    if (leftMost) {
      next_page_id = ip->ValueAt(0);
    } else {
      next_page_id = ip->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    page_id = next_page_id;
    bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAndUnpin(enum OpType op, Transaction *transaction) {
  auto pages = transaction->GetPageSet();
  for (auto page : *pages) {
    page_id_t page_id = page->GetPageId();
    if (op == OpType::READ) {
//      LOG_INFO("RUnlock %d", page->GetPageId());
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
    } else {
//      LOG_INFO("%d WUnlock %d", transaction->GetTransactionId(), page->GetPageId());
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
  }
//  LOG_INFO("%d", root_page_id_);
  pages->clear();

  auto page_ids = transaction->GetDeletedPageSet();
  for (auto page_id : *page_ids) {
//    LOG_INFO("Delete %d", page_id);
    buffer_pool_manager_->DeletePage(page_id);
  }
  page_ids->clear();
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafe(BPlusTreePage* bptp, enum OpType op) {
  if (op == OpType::INSERT) {
     return bptp->GetSize() < bptp->GetMaxSize() - 1;
  }

  return bptp->GetSize() > bptp->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageWithLock(const KeyType &key, enum OpType op, Transaction *transaction, bool left_most) {
  page_id_t page_id = root_page_id_;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
//  LOG_INFO("%d PAGE ID %d", root_page_id_, page->GetPageId());
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!bptp->IsLeafPage()) {
    if (op == OpType::READ) {
//      LOG_INFO("RLock %d", page->GetPageId());
      page->RLatch();
      ReleaseAndUnpin(op, transaction);
    } else {
//      LOG_INFO("%d WLock %d", transaction->GetTransactionId(), page->GetPageId());
      page->WLatch();
      if (IsSafe(bptp, op)) {
//        LOG_INFO("Safe");
        ReleaseAndUnpin(op, transaction);
      }
    }
    transaction->AddIntoPageSet(page);
    auto ip = reinterpret_cast<InternalPage *>(bptp);
    page_id_t next_page_id = INVALID_PAGE_ID;
    if (left_most) {
      next_page_id = ip->ValueAt(0);
    } else {
      next_page_id = ip->Lookup(key, comparator_);
    }
//    LOG_INFO("%d next_page_id %d", page->GetPageId(), next_page_id);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    page_id = next_page_id;
    bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  if (op == OpType::READ) {
    page->RLatch();
//    LOG_INFO("RLock %d", page->GetPageId());
    ReleaseAndUnpin(op, transaction);
  } else {
    page->WLatch();
//    LOG_INFO("%d lalalaWLock %d", transaction->GetTransactionId(), page->GetPageId());
    if (IsSafe(bptp, op)) {
//      LOG_INFO("Safe");
      ReleaseAndUnpin(op, transaction);
    }
  }
  transaction->AddIntoPageSet(page);
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
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
 * This method is used for debug only, You don't  need to modify
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
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

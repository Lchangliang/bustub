//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page *page = nullptr;
  {
    std::lock_guard<std::mutex> guard(latch_);
    auto iter = page_table_.find(page_id);
    if (iter != page_table_.end()) {
      Page *page = &pages_[iter->second];
      if (page->pin_count_ == 0) {
        replacer_->Pin(iter->second);
      }
      page->pin_count_++;
      return page;
    }
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }
    page = InitNewPage(page_id);
    disk_manager_->ReadPage(page_id, page->GetData());
  }
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() <= 0) {
    return false;
  }
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  if (pages_[iter->second].is_dirty_) {
    disk_manager_->WritePage(pages_[iter->second].page_id_, pages_[iter->second].GetData());
    pages_[iter->second].is_dirty_ = false;
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  Page *page = nullptr;
  {
    std::lock_guard<std::mutex> guard(latch_);
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }
    *page_id = AllocatePage();
    page = InitNewPage(*page_id);
    disk_manager_->WritePage(page->page_id_, page->GetData());
  }
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = iter->second;
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  replacer_->Pin(frame_id);
  page_table_.erase(page_id);
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    std::lock_guard<std::mutex> guard(latch_);
    if (pages_[i].page_id_ != INVALID_PAGE_ID && pages_[i].is_dirty_) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::InitNewPage(page_id_t page_id) {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = *free_list_.begin();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
    page_table_.erase(pages_[frame_id].page_id_);
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
    }
  }
  Page *page = &pages_[frame_id];
  page->ResetMemory();
  page->is_dirty_ = false;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page_table_[page_id] = frame_id;
  return page;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  return next_page_id_++;;
}

}  // namespace bustub

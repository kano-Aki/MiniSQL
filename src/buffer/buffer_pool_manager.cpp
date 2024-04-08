#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer();
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    if(page_id==INVALID_PAGE_ID) return nullptr;//cerr<<"Pin "<<page_id<<' '<<pool_size_<<'\n';
    unordered_map<page_id_t, frame_id_t>::iterator table_it;
    frame_id_t deleted_frame_id;
    table_it=page_table_.find(page_id);
    if(table_it!=page_table_.end())//find it
    {//cerr<<"Pin frame "<<table_it->second<<' '<<pages_[7].pin_count_<<'\n';
        replacer_->Pin(table_it->second);
        pages_[table_it->second].pin_count_++;//the number of thread add 1
        return pages_+table_it->second;//return pointer
    }
    else
    {
        if(free_list_.empty())
        {
            if(!replacer_->Victim(&deleted_frame_id)) return nullptr;//no replaced page
        }
        else
        {
            deleted_frame_id=*(free_list_.begin());//just use the first free page
            free_list_.remove(deleted_frame_id);
        }
        if(pages_[deleted_frame_id].is_dirty_)//should write back first
        {
            disk_manager_->WritePage(pages_[deleted_frame_id].page_id_,pages_[deleted_frame_id].data_);//that is FlushPage()
        }
        page_table_.erase(pages_[deleted_frame_id].page_id_);
        page_table_.insert(make_pair(page_id,deleted_frame_id));//Delete R from the page table and insert P.
        pages_[deleted_frame_id].ResetMemory();
        pages_[deleted_frame_id].page_id_=page_id;
        pages_[deleted_frame_id].is_dirty_=false;
        pages_[deleted_frame_id].pin_count_=1;//update P
        replacer_->Pin(deleted_frame_id);//pin it
        disk_manager_->ReadPage(page_id,pages_[deleted_frame_id].data_);//read P
        return pages_+deleted_frame_id;
    }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    frame_id_t deleted_frame_id;
    if(free_list_.empty())
    {
        if(!replacer_->Victim(&deleted_frame_id)) return nullptr;
    }
    else
    {
        deleted_frame_id=*(free_list_.begin());
        free_list_.remove(deleted_frame_id);
    }
    if(pages_[deleted_frame_id].is_dirty_==true) FlushPage(deleted_frame_id);//write back first
    page_id=AllocatePage();//cerr<<"Pin "<<page_id<<' '<<pool_size_<<'\n';
    pages_[deleted_frame_id].is_dirty_=false;
    pages_[deleted_frame_id].page_id_=page_id;
    pages_[deleted_frame_id].pin_count_=1;
    pages_[deleted_frame_id].ResetMemory();
    page_table_.erase(deleted_frame_id);//delete the replaced page in table 
    page_table_.insert(make_pair(page_id,deleted_frame_id));
    replacer_->Pin(deleted_frame_id);
    return pages_+deleted_frame_id;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    frame_id_t deleted_frame_id;
    if(page_table_.find(page_id)!=page_table_.end())//find it
    {
        deleted_frame_id=page_table_.find(page_id)->second;
        if(pages_[deleted_frame_id].pin_count_>0) return false;
        DeallocatePage(page_id);
        page_table_.erase(page_id);
        free_list_.emplace_back(deleted_frame_id);//add to free list
        pages_[deleted_frame_id].ResetMemory();
        pages_[deleted_frame_id].page_id_=INVALID_PAGE_ID;
        pages_[deleted_frame_id].is_dirty_=false;
        pages_[deleted_frame_id].pin_count_=0;//reset P
    }
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    frame_id_t frame_id;
    if(page_table_.find(page_id)==page_table_.end()) return false;//no pined page
    frame_id=page_table_.find(page_id)->second;
    if(pages_[frame_id].pin_count_==0) return false;//not pin
    pages_[frame_id].pin_count_--;
    pages_[frame_id].is_dirty_=is_dirty;//update status
    if(pages_[frame_id].pin_count_==0) replacer_->Unpin(frame_id);//after update,if pin_count is 0,then unpin it in replacer
    return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    frame_id_t frame_id;
    frame_id=page_table_.find(page_id)->second;
    disk_manager_->WritePage(page_id,pages_[frame_id].data_);
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " <<i<<' '<< pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}

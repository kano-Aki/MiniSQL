#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  // if (current_page_id != INVALID_PAGE_ID)
  // page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  // if (current_page_id != INVALID_PAGE_ID)
  //   buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  auto page=buffer_pool_manager->FetchPage(current_page_id);
  auto node=reinterpret_cast<LeafPage*>(page->GetData());
  auto ret=std::pair{node->KeyAt(item_index),node->ValueAt(item_index)};
  buffer_pool_manager->UnpinPage(current_page_id,1);
  return ret;
  ASSERT(false, "Not implemented yet.");
}

IndexIterator &IndexIterator::operator++() {
  page_id_t tmp=current_page_id;
  auto page=buffer_pool_manager->FetchPage(tmp);
  auto node=reinterpret_cast<LeafPage*>(page->GetData());
  if(item_index<node->GetSize())
  this->item_index++;
  else
  this->current_page_id=node->GetNextPageId(),item_index=1;
  // cerr<<"after++ "<<current_page_id<<' '<<item_index<<'\n';
  buffer_pool_manager->UnpinPage(tmp,1);
  return *this;
  ASSERT(false, "Not implemented yet.");
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}
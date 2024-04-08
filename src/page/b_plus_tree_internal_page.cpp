#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()
// #pragma GCC optimize("Ofast")

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  if(key_size==UNDEFINED_SIZE)
  SetKeySize(256);
  else
  this->SetKeySize(key_size);
  if(max_size==UNDEFINED_SIZE)
  SetMaxSize(128);
  else
  this->SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}
int InternalPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int l=1,r=GetSize()-1;
  while(l<r)
  {
    int mid=l+r>>1;
    if(KM.CompareKeys(KeyAt(mid),key)>=0)
    r=mid;
    else
    l=mid+1;
  }
  return r;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int l=1,r=this->GetSize();
  // cerr<<"Internal: Lookup "<<l<<' '<<r<<'\n';
  KM.CompareKeys(key,key);
  // cerr<<"IL: CC\n";
  while(l<r)
  {
    int mid=l+r>>1;
    // cerr<<"Internal: Lookup "<<mid<<'\n';
    KM.CompareKeys(KeyAt(mid),KeyAt(mid));
    // cerr<<"ILm: CC\n";
    if(KM.CompareKeys(KeyAt(mid),key)>0)
    r=mid;
    else
    l=mid+1;
  }
  // cerr<<"Internal: Lookup finish "<<r-1<<'\n';
  return ValueAt(r-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(2);
  SetValueAt(0,old_value);
  SetKeyAt(1,new_key);
  SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetSize(GetSize()+1);
  for(int i=GetSize()-1;i;i--)
  {
    if(ValueAt(i-1)==old_value)
    {
      SetKeyAt(i,new_key),SetValueAt(i,new_value);
      break;
    }
    else
    SetKeyAt(i,KeyAt(i-1)),SetValueAt(i,ValueAt(i-1));
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size=GetSize();
  recipient->CopyNFrom(PairPtrAt((size>>1)),size+1>>1,buffer_pool_manager);
  SetSize(size>>1);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  SetSize(size);
  PairCopy(PairPtrAt(0),src,size);
  for(int i=0;i<size;i++)
  {
    auto page=buffer_pool_manager->FetchPage(ValueAt(i));
    auto node=reinterpret_cast<InternalPage*> (page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(i),1);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int size=GetSize();
  for(int i=index;i<size-1;i++)
  {
    SetKeyAt(i,KeyAt(i+1));
    SetValueAt(i,ValueAt(i+1));
  }
  SetSize(size-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int size=GetSize();
  recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);
  for(int i=1;i<size;i++)
  recipient->CopyLastFrom(KeyAt(i),ValueAt(i),buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);
  int size=GetSize();
  for(int i=0;i<size-1;i++)
  PairCopy(PairPtrAt(i),PairPtrAt(i+1));
  SetSize(size-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetSize(GetSize()+1);
  SetKeyAt(GetSize()-1,key);
  SetValueAt(GetSize()-1,value);
  auto page=buffer_pool_manager->FetchPage(value);
  auto node=reinterpret_cast<InternalPage*> (page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value,1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
GenericKey* InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  recipient->CopyFirstFrom(ValueAt(size-1),buffer_pool_manager);
  recipient->SetKeyAt(1,middle_key);
  // cerr<<"MoveLastToFrontOf check\n";
  SetSize(size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size=GetSize();
  SetSize(size+1);
  for(int i=size;i;i--)
  PairCopy(PairPtrAt(i),PairPtrAt(i-1));
  SetValueAt(0,value);
  auto page=buffer_pool_manager->FetchPage(value);
  auto node=reinterpret_cast<InternalPage*> (page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value,1);
}
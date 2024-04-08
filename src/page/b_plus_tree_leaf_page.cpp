#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
// #pragma GCC optimize("Ofast")
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  if(key_size==UNDEFINED_SIZE)
  SetKeySize(256);
  else
  SetKeySize(key_size);
  if(max_size==UNDEFINED_SIZE)
  SetMaxSize(128);
  else
  SetMaxSize(max_size);
  SetNextPageId(-1);
  SetPrevPageId(-1);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

page_id_t LeafPage::GetPrevPageId() const {
  return prev_page_id_;
}

void LeafPage::SetPrevPageId(page_id_t next_page_id) {
  prev_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int l=1,r=GetSize();
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

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(nullptr, RowId());
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM,BufferPoolManager* bpm) {
  SetSize(GetSize()+1);
  // cerr<<"Leaf::Insert\n";
  // bpm->testt();
  for(int i=GetSize()-1;~i;i--)
  {
    // cerr<<"LI "<<i<<'\n';
    // bpm->testt();
    if(i==0||KM.CompareKeys(key,KeyAt(i))>0)
    {
      // bpm->testt();
      SetKeyAt(i+1,key),SetValueAt(i+1,value);
      // bpm->testt();
      break;
    }
    else
    {
      // bpm->testt();
      // SetKeyAt(i+1,KeyAt(i)),SetValueAt(i+1,ValueAt(i));
      PairCopy(PairPtrAt(i+1),PairPtrAt(i),1);
      // bpm->testt();
    }
  }
  // bpm->testt();
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size=GetSize();
  recipient->CopyNFrom(PairPtrAt((size>>1)+1),size+1>>1);
  SetSize(size>>1);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  SetSize(size);
  PairCopy(PairPtrAt(1),src,size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  if(GetSize()==0)
  return false;
  // cerr<<"LeafLookup:Lion "<<GetSize()<<"\n";
  int l=1,r=this->GetSize();
  // cerr<<"LeafLookup:Cat\n";
  while(l<r)
  {
    int mid=l+r>>1;
    // cerr<<"LeafLookup:Tiger\n";
    if(KM.CompareKeys(KeyAt(mid),key)>=0)
    r=mid;
    else
    l=mid+1;
  }
  // cerr<<"LeafLookup:Leopard "<<r<<"\n";
  if(KM.CompareKeys(key,KeyAt(r))==0)
  {
    value=ValueAt(r);
    return true;
  }
  // cerr<<"LeafLookup:Shear ";
  // for(int i=1;i<=GetSize();i++)
  // cerr<<KM.CompareKeys(key,KeyAt(i))<<' ';
  // cerr<<'\n';
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int l=1,r=this->GetSize();
  // cerr<<"RemoveAndDeleteRecord: "<<l<<' '<<r<<'\n';
  while(l<r)
  {
    int mid=l+r>>1;
    if(KM.CompareKeys(KeyAt(mid),key)>=0)
    r=mid;
    else
    l=mid+1;
  }
  // cerr<<"RemoveAndDeleteRecord: finish"<<r<<'\n';
  if(KM.CompareKeys(key,KeyAt(r))==0)
  {
    int size=GetSize();
    for(int i=r;i<size;i++)
    PairCopy(PairPtrAt(i),PairPtrAt(i+1));
    SetSize(size-1);
    return size-1;
  }
  // cerr<<"RemoveAndDeleteRecord: Not Found!!!\n";
  // for(int i=1;i<=GetSize();i++)
  // cerr<<KM.CompareKeys(key,KeyAt(i))<<' ';
  // cerr<<'\n';
  return -1;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  int size=GetSize();
  for(int i=1;i<=size;i++)
  recipient->CopyLastFrom(KeyAt(i),ValueAt(i));
  // recipient->SetNextPageId(GetPageId());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(KeyAt(1),ValueAt(1));
  int size=GetSize();
  for(int i=1;i<size;i++)
  PairCopy(PairPtrAt(i),PairPtrAt(i+1));
  SetSize(size-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetSize(GetSize()+1);
  SetKeyAt(GetSize(),key);
  SetValueAt(GetSize(),value);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int size=GetSize();
  recipient->CopyFirstFrom(KeyAt(size),ValueAt(size));
  SetSize(size-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int size=GetSize();
  SetSize(size+1);
  for(int i=size;i;i--)
  PairCopy(PairPtrAt(i+1),PairPtrAt(i));
  SetKeyAt(1,key);
  SetValueAt(1,value);
}
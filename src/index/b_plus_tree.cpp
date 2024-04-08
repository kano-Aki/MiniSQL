#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
// #pragma GCC optimize(2)

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM)
      // leaf_max_size_(leaf_max_size),
      // internal_max_size_(internal_max_size) 
      {
        leaf_max_size_=(PAGE_SIZE-32)/(KM.GetKeySize()+sizeof(RowId))-2;
        internal_max_size_=(PAGE_SIZE-32)/(KM.GetKeySize()+sizeof(page_id_t))-1;
        IndexRootsPage* page=reinterpret_cast<IndexRootsPage*>(
          buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData()
        );
        if(!page->GetRootId(index_id_,&root_page_id_))
        root_page_id_=INVALID_PAGE_ID;//cerr<<"BPTree Init: root id not found\n";
        buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,1);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if(IsEmpty())
  {
    return false;
  }
  auto &bpm=buffer_pool_manager_;auto &KM=processor_;
  auto page=FindLeafPage(key,root_page_id_);
  LeafPage* u=reinterpret_cast<BPlusTreeLeafPage *> (page->GetData());
  auto uid=u->GetPageId();
  RowId tmp;
  // cerr<<"GetValue: to lookup\n";
  if(!u->Lookup(key,tmp,KM))
  {
    // result.push_back(INVALID_ROWID);
    bpm->UnpinPage(uid,0);
    return false;
  }
  // cerr<<"GetValue: lookup finish\n";
  result.push_back(tmp);
  bpm->UnpinPage(uid,0);
  // cerr<<"GetValue: Done\n";
  return true;
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if(IsEmpty())
  {
    StartNewTree(key,value);
    return 1;
  }
  auto &bpm=buffer_pool_manager_;auto &KM=processor_;
  auto page=FindLeafPage(key,root_page_id_);
  BPlusTreePage* node=reinterpret_cast<BPlusTreePage *>(page->GetData());
  LeafPage* u=reinterpret_cast<BPlusTreeLeafPage *> (page->GetData());
  auto uid=u->GetPageId();
  // cerr<<"BTIhii\n";
  // bpm->testt();
  auto tmp=value;
  if(u->Lookup(key,tmp,KM))
  {
    bpm->UnpinPage(uid,0);
    return false;
  }
  // cerr<<"BTIhoo\n";
  // bpm->testt();
  // cerr<<uid<<'\n';
  // bpm->testt();
  int size=u->Insert(key,value,KM,bpm);
  // bpm->testt();
  // cerr<<"size after insert: "<<size<<'\n';
  // bpm->testt();
  if(size>node->GetMaxSize())
  {
    // cerr<<"BTIhuu"<<size<<' '<<node->GetMaxSize()<<"\n";
    GenericKey* midkey=u->KeyAt((size>>1)+1);
    InsertIntoParent(node,midkey,Split(u,transaction));
  }
  // bpm->testt();
  bpm->UnpinPage(uid,1);
  // bpm->testt();
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto& bpm=buffer_pool_manager_;auto &KM=processor_;
  page_id_t page_id;
  // cerr<<"cyw\n";
  auto page=bpm->NewPage(page_id);
  // cerr<<"zjy\n";
  if(page==nullptr)
  {
    cerr<<"out of memory\n";
    exit(0);
  }
  // cerr<<"cywzjy\n";
  auto node=reinterpret_cast<LeafPage*> (page->GetData());
  // cerr<<"zjycyw\n";
  node->Init(page_id,INVALID_PAGE_ID,KM.GetKeySize(),leaf_max_size_);
  // cerr<<"cywcyw\n";
  node->SetSize(0);
  // cerr<<"zjyzjy\n";
  root_page_id_=page_id;
  bpm->UnpinPage(page_id,1);
  // cerr<<"cywhh\n";
  UpdateRootPageId(1);
  // cerr<<"hhcyw\n";
  InsertIntoLeaf(key,value);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  auto& bpm=buffer_pool_manager_;auto &KM=processor_;
  auto page=bpm->FetchPage(root_page_id_);
  auto node=reinterpret_cast<LeafPage*> (page->GetData());
  // cerr<<"nongnong\n";
  RowId tmp;
  // cerr<<"zhouzhou\n";
  if(node->Lookup(key,tmp,KM))
  {
    // cerr<<"yuanyuan\n";
    bpm->UnpinPage(root_page_id_,1);
    return false;
  }
  // cerr<<"xingxing\n";
  node->Insert(key,value,KM,bpm);
  // cerr<<"baba\n";
  bpm->UnpinPage(root_page_id_,1);
  // cerr<<"mama "<<root_page_id_<<"\n";
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t newer;
  auto page=buffer_pool_manager_->NewPage(newer);
  if(page==nullptr)
  return nullptr;
  auto nnode=reinterpret_cast<InternalPage*> (page->GetData());
  nnode->Init(newer,node->GetParentPageId(),node->GetKeySize(),internal_max_size_);
  node->MoveHalfTo(nnode,buffer_pool_manager_);
  // buffer_pool_manager_->UnpinPage(nnode->GetPageId(),1);
  return nnode;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t newer;
  auto page=buffer_pool_manager_->NewPage(newer);
  // cerr<<"LeafSplit: pin "<<newer<<'\n';
  if(page==nullptr)
  return nullptr;
  auto nnode=reinterpret_cast<LeafPage*> (page->GetData());
  nnode->Init(newer,node->GetParentPageId(),node->GetKeySize(),leaf_max_size_);
  node->MoveHalfTo(nnode);
  // cerr<<"LeafSplit: leftsize="<<node->GetSize()<<",rightsize="<<nnode->GetSize()<<'\n';

  nnode->SetNextPageId(node->GetNextPageId());
  if(~node->GetNextPageId())
  {
    auto nxt=node->GetNextPageId();
    auto nxtnode=reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(nxt)->GetData());
    nxtnode->SetPrevPageId(newer);
    buffer_pool_manager_->UnpinPage(nxt,1);
  }
  nnode->SetPrevPageId(node->GetPageId());
  node->SetNextPageId(newer);
  // buffer_pool_manager_->UnpinPage(nnode->GetPageId(),1);
  return nnode;
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
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  auto& bpm=buffer_pool_manager_;auto &KM=processor_;
  // cerr<<"IIP0\n";
  if(old_node->IsRootPage())
  {
    // cerr<<"IIP1.5\n";
    page_id_t newrt;
    auto rtnode=bpm->NewPage(newrt);
    auto rt=reinterpret_cast<InternalPage*>(rtnode->GetData());
    rt->Init(newrt,INVALID_PAGE_ID,KM.GetKeySize(),internal_max_size_);
    page_id_t old_value,new_value;
    if(old_node->IsLeafPage())
    {
      old_value=old_node->GetPageId();
      new_value=new_node->GetPageId();
    }
    else
    {
      old_value=reinterpret_cast<InternalPage*>(old_node)->ValueAt(0);
      new_value=reinterpret_cast<InternalPage*>(new_node)->ValueAt(0);
    }
    // cerr<<"IIP2.5\n";
    rt->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(newrt);
    new_node->SetParentPageId(newrt);
    KM.CompareKeys(key,key);
    // cerr<<"IIP3.4\n";
    KM.CompareKeys(rt->KeyAt(1),rt->KeyAt(1));
    // cerr<<"IIP3.5\n";
    root_page_id_=newrt;
    bpm->UnpinPage(newrt,1);
    UpdateRootPageId(0);
    bpm->UnpinPage(new_node->GetPageId(),1);
    return;
  }
  page_id_t fa_id=old_node->GetParentPageId();
  // cerr<<"IIP1\n";
  auto page=bpm->FetchPage(fa_id);
  auto fa=reinterpret_cast<InternalPage*> (page->GetData());
  // cerr<<"IIP2\n";
  fa->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
  // old_node->SetParentPageId(fa_id);
  new_node->SetParentPageId(fa_id);
  // cerr<<"IIP3\n";
  if(fa->GetSize()>fa->GetMaxSize())
  {
    // cerr<<"IIP4\n";
    auto midkey=fa->KeyAt(fa->GetSize()>>1);
    auto newer=Split(fa,transaction);
    // cerr<<"IIP5\n";
    KM.CompareKeys(midkey,midkey);
    // cerr<<"IIP6\n";
    InsertIntoParent(fa,midkey,newer,transaction);
  }
  bpm->UnpinPage(new_node->GetPageId(),1);
  bpm->UnpinPage(fa_id,1);
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
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if(IsEmpty())
  return;
  auto &bpm=buffer_pool_manager_;auto &KM=processor_;
  auto page=FindLeafPage(key,root_page_id_);
  BPlusTreePage* node=reinterpret_cast<BPlusTreePage *>(page->GetData());
  LeafPage* u=reinterpret_cast<BPlusTreeLeafPage *> (page->GetData());
  auto uid=u->GetPageId();

  RowId tmp;
  if(KM.CompareKeys(key,u->KeyAt(1))==0)
  {
    auto new_key=u->KeyAt(2);
    // cerr<<"REREMOVE\n";
    for(int x=uid,y=u->GetParentPageId();~y;)
    {
      // cerr<<x<<'\n';
      auto anc=reinterpret_cast<InternalPage*>(bpm->FetchPage(y)->GetData());
      if (KM.CompareKeys(key, anc->KeyAt(1)) == -1)
      {
        auto index = anc->KeyIndex(key,KM);
        auto now_key = anc->KeyAt(index);
        if (index && KM.CompareKeys(now_key, key) == 0) {
          anc->SetKeyAt(index, new_key);
          bpm->UnpinPage(y, 1);
          break;
        }
      }
      x=y;
      y=anc->GetParentPageId();
      bpm->UnpinPage(x,0);
    }
  }
  // cerr<<"REREREMOVE\n";
  int size=u->RemoveAndDeleteRecord(key,KM);
  if(size==-1)
  return;
  if(size<u->GetMinSize()&&!node->IsRootPage())
  {
    // page_id_t nxt=u->GetNextPageId();
    // if(~nxt)
    // {
    //   auto page=bpm->FetchPage(nxt);
    //   auto nxtnode=reinterpret_cast<LeafPage*> (page->GetData());
    //   if(nxtnode->GetSize()>nxtnode->GetMinSize())
    //   nxtnode->MoveFirstToEndOf(node);
    //   else
    //   nxtnode->MoveAllTo(node);
    //   bpm->UnpinPage(nxt);
    // }
    CoalesceOrRedistribute<LeafPage>(u,transaction);
  }
  if(node->IsRootPage()&&size==0)
  {
    if(AdjustRoot(node))
    return;
  }
  // for(int x=uid,y=u->GetParentPageId();~y;)
  // {
  //   cerr<<y<<'\n';
  //   auto anc=reinterpret_cast<InternalPage*>(bpm->FetchPage(y)->GetData());
  //   for(int i=1;i<anc->GetSize();i++)
  //   {
  //     auto page=FindLeafPage(anc->KeyAt(i),y);
  //     auto firid=reinterpret_cast<LeafPage*>(page->GetData())->GetPageId();
  //     bpm->UnpinPage(firid,0);
  //     page=FindLeafPage(anc->KeyAt(i),anc->ValueAt(i),1);
  //     auto secid=reinterpret_cast<LeafPage*>(page->GetData())->GetPageId();
  //     bpm->UnpinPage(secid,0);
  //     cerr<<firid<<' '<<secid<<'\n';
  //     assert(firid==secid);
  //   }
    
  //   x=y;
  //   y=anc->GetParentPageId();
  //   bpm->UnpinPage(x,1);
  // }
  bpm->UnpinPage(uid,1);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  auto fa_id=node->GetParentPageId();
  auto fa=reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(fa_id)->GetData());
  auto cur=fa->ValueIndex(node->GetPageId());
  int nid=cur?cur-1:cur+1;
  page_id_t nxt = fa->ValueAt(nid);
  if (~nxt)
  {
    auto page = buffer_pool_manager_->FetchPage(nxt);
    N * nxtnode = reinterpret_cast<N*>(page->GetData());
    // cerr<<"CoalesceOrRedistribute "<<node->GetPageId()<<':'<<node->GetSize()<<';'<<nxt<<':'<<nxtnode->GetSize()<<'\n';
    if (nxtnode->GetSize() + node->GetSize() <= node->GetMaxSize())
    {
      if(Coalesce(node,nxtnode,fa,cur,transaction))
      {
        if(!fa->IsRootPage())
        CoalesceOrRedistribute(fa,transaction);
        else
        {
          if(AdjustRoot(fa))
          node->SetParentPageId(INVALID_PAGE_ID);
        }
      }
      buffer_pool_manager_->UnpinPage(fa->GetPageId(),1);
      return true;
    }
    else
    {
      // cerr<<"CoalesceOrRedistribute: case3\n";
      Redistribute(node,nxtnode,cur);
      buffer_pool_manager_->UnpinPage(fa->GetPageId(),1);
    }
    buffer_pool_manager_->UnpinPage(nxt,1);
  }
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
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int cur,
                         Transaction *transaction) {
  // cerr<<"Coalesce Leaf\n";
  if(cur)//node---neighbor_node
  {
    // cerr<<"Coalesce Case 1\n";
    if(~node->GetPrevPageId())
    {
      auto pre=node->GetPrevPageId();
      auto prenode=reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(pre)->GetData());
      prenode->SetNextPageId(neighbor_node->GetPageId());
      buffer_pool_manager_->UnpinPage(pre,1);
    }
    neighbor_node->SetPrevPageId(node->GetPrevPageId());

    while(node->GetSize())
    {
      parent->SetKeyAt(cur,node->KeyAt(node->GetSize()));
      node->MoveLastToFrontOf(neighbor_node);
    }
    parent->Remove(cur-1);
  }
  else//neighbor_node---node
  {
    // cerr<<"Coalesce Case 2\n";
    
    if(~node->GetNextPageId())
    {
      auto nxt=node->GetNextPageId();
      auto nxtnode=reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(nxt)->GetData());
      nxtnode->SetPrevPageId(neighbor_node->GetPageId());
      buffer_pool_manager_->UnpinPage(nxt,1);
    }
    neighbor_node->SetNextPageId(node->GetNextPageId());

    node->MoveAllTo(neighbor_node);
    parent->Remove(cur+1);
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(),1);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  // cerr<<"Coalesce Leaf: Comparing neighbor\n";
  // for(int i=2;i<=neighbor_node->GetSize();i++)
  // assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
  // cerr<<"Coalesce Leaf: Comparing parent\n";
  // for(int i=2;i<parent->GetSize();i++)
  // assert(processor_.CompareKeys(parent->KeyAt(i-1),parent->KeyAt(i))==-1);
  if(parent->GetSize()<parent->GetMinSize()&&!parent->IsRootPage())
  return true;
  if(parent->GetSize()<2&&parent->IsRootPage())
  return true;
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int cur,
                         Transaction *transaction) {
  // node->MoveAllTo(neighbor_node,parent->KeyAt(index),buffer_pool_manager_);
  // parent->Remove(index);
  // cerr<<"Coalesce Internal"<<parent->GetPageId()<<' '<<parent->GetSize()<<' '<<cur<<"\n";
  if(cur)//node---neighbor_node
  {
    // cerr<<"Coalesce Case 1\n";
    assert(cur<parent->GetSize());
    GenericKey* midkey=(parent->KeyAt(cur));
    // cerr<<"Coalesce Internal: Comparing node\n";
    // for(int i=2;i<node->GetSize();i++)
    // assert(processor_.CompareKeys(node->KeyAt(i-1),node->KeyAt(i))==-1);
    // cerr<<"Coalesce Internal: Comparing neighbornode\n";
    // for(int i=2;i<neighbor_node->GetSize();i++)
    // assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
    while(node->GetSize())
    {
      // parent->SetKeyAt(cur,node->KeyAt(node->GetSize()-1));
      auto lastkey=node->KeyAt(node->GetSize()-1);
      node->MoveLastToFrontOf(neighbor_node,midkey,buffer_pool_manager_);
        // cerr<<"Coalesce Internal: Comparing neighbornode again "<<node->GetSize()<<"\n";
        // for(int i=2;i<neighbor_node->GetSize();i++)
        // assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
      midkey=lastkey;
    }
    parent->SetKeyAt(cur,parent->KeyAt(cur-1));
    parent->Remove(cur-1);
    // cerr<<"Coalesce Internal: Comparing neighbornode again\n";
    // for(int i=2;i<neighbor_node->GetSize();i++)
    // assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
  }
  else
  {
    // cerr<<"Coalesce Case 2\n";
    // processor_.CompareKeys(parent->KeyAt(cur+1),parent->KeyAt(cur+1));
    node->MoveAllTo(neighbor_node,parent->KeyAt(cur+1),buffer_pool_manager_);
    parent->Remove(cur+1);
  }
  // cerr<<"Coalesce Internal: Move Finish\n";
  buffer_pool_manager_->UnpinPage(node->GetPageId(),1);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  // cerr<<"Coalesce Internal: Delete Finish\n";
  // for(int i=1;i<neighbor_node->GetSize();i++)
  // cerr<<"$$"<<i<<'\n',processor_.CompareKeys(neighbor_node->KeyAt(i),neighbor_node->KeyAt(i));
  // for(int i=2;i<neighbor_node->GetSize();i++)
  // cerr<<"$$"<<i<<'\n',assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
  // cerr<<"Coalesce Internal: Assert Finish\n";
  if(parent->GetSize()<parent->GetMinSize()&&!parent->IsRootPage())
  return true;
  if(parent->GetSize()<2&&parent->IsRootPage())
  return true;
  return false;
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
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent=reinterpret_cast<InternalPage*> (parent_page->GetData());
  // cerr<<"Redistribute Leaf\n";
  if(index)//node--neighbor_node
  {
    node->MoveLastToFrontOf(neighbor_node);
    parent->SetKeyAt(index,neighbor_node->KeyAt(1));
  }
  else
  {
    node->MoveFirstToEndOf(neighbor_node);
    parent->SetKeyAt(index+1,node->KeyAt(1));
  }
  // for(int i=2;i<=neighbor_node->GetSize();i++)
  // assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),1);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent=reinterpret_cast<InternalPage*> (parent_page->GetData());
  auto middle_key=parent->KeyAt(index);
  // cerr<<"Redistribute Internal\n";
  if(index)//node--neighbor_node
  {
    // cerr<<"Redistribute Internal Case 1\n";
    auto lastkey=node->KeyAt(node->GetSize()-1);
    node->MoveLastToFrontOf(neighbor_node,middle_key,buffer_pool_manager_);
    parent->SetKeyAt(index,lastkey);
  }
  else
  {
    // cerr<<"Redistribute Internal Case 2\n";
    middle_key=parent->KeyAt(index+1);
    auto secondkey=node->KeyAt(1);
    node->MoveFirstToEndOf(neighbor_node,middle_key,buffer_pool_manager_);
    parent->SetKeyAt(index+1,node->KeyAt(0));
  }
  // node->MoveFirstToEndOf(neighbor_node,middle_key,buffer_pool_manager_);
  // parent->SetKeyAt(index,node->KeyAt(0));
  // for(int i=2;i<neighbor_node->GetSize();i++)
  // assert(processor_.CompareKeys(neighbor_node->KeyAt(i-1),neighbor_node->KeyAt(i))==-1);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),1);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // cerr<<"AdjustRoot "<<old_root_node->GetSize()<<"\n";
  if(old_root_node->GetSize()==1&&old_root_node->IsLeafPage()==0)
  {
    InternalPage* node=reinterpret_cast<InternalPage*> (old_root_node);
    root_page_id_=node->RemoveAndReturnOnlyChild();
    UpdateRootPageId();
    return 1;
  }
  // cerr<<"AdjjustRoot\n";
  if(old_root_node->GetSize()==0)
  {
    buffer_pool_manager_->UnpinPage(root_page_id_,1);
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_=INVALID_PAGE_ID,UpdateRootPageId();//cerr<<IsEmpty()<<"RR\n";
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  if(IsEmpty())
  {
    return End();
  }
  // cerr<<"Begin\n";
  page_id_t now=root_page_id_;
  auto page=FindLeafPage(new GenericKey,now,1);
  BPlusTreePage* node=reinterpret_cast<BPlusTreePage *>(page->GetData());
  LeafPage* u=reinterpret_cast<BPlusTreeLeafPage *> (page->GetData());
  // cerr<<"Begin return\n";
  now=u->GetPageId();
  buffer_pool_manager_->UnpinPage(now,0);
  return IndexIterator(now,buffer_pool_manager_,1);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  if(IsEmpty())
  {
    return End();
  }
  page_id_t now=root_page_id_;
  auto page=FindLeafPage(key,now,0);
  BPlusTreePage* node=reinterpret_cast<BPlusTreePage *>(page->GetData());
  LeafPage* u=reinterpret_cast<BPlusTreeLeafPage *> (page->GetData());
  // cerr<<"Begin return\n";
  now=u->GetPageId();
  auto index=u->KeyIndex(key,processor_);
  buffer_pool_manager_->UnpinPage(now,0);
  return IndexIterator(now,buffer_pool_manager_,index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID,buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {


  page_id_t now=page_id;
  auto& bpm=buffer_pool_manager_;auto &KM=processor_;
  // cerr<<"FindLeafPagehaa\n";
  auto page=bpm->FetchPage(now);
  // cerr<<"FindLeafPagehee\n";
  BPlusTreeInternalPage * node=reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  vector<page_id_t> stak;
  stak.push_back(now);
  // auto Nxt=[&](BPlusTreeInternalPage * u)
  // {
  // };
  while(!node->IsLeafPage())
  {
    // BPlusTreeInternalPage * u=reinterpret_cast<BPlusTreeInternalPage *> (node);
    // Nxt(u);
    if(leftMost)
    now=node->ValueAt(0);
    else
    now=node->Lookup(key,KM);
    page=bpm->FetchPage(now);
    node=reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    stak.push_back(now);
  }
  for(auto& x:stak)
  if(x!=node->GetPageId())
  bpm->UnpinPage(x,0);
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto index_page=buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage* index_node=reinterpret_cast<IndexRootsPage*> (index_page->GetData());
  if(insert_record)
  {
    index_node->Insert(index_id_,root_page_id_);
  }
  else
  {
    index_node->Update(index_id_,root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,1);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
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
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
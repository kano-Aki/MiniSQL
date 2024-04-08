#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    TablePage* first=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(now_insert_page_id));
    bool sign=false;
    // cerr<<"InsertTuple1\n";
    //  cerr<<"InsertTuple2\n";
    first->WLatch();
    // cerr<<"InsertTuple2.2\n";
    // assert(first!=nullptr);
    // assert(schema_!=nullptr);
    // cerr<<now_insert_page_id<<'\n';
    sign=first->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
    // cerr<<"InsertTuple2.5\n";
    first->WUnlatch();
    // cerr<<"InsertTuple2.8\n";
    buffer_pool_manager_->UnpinPage(first->GetPageId(),sign);//after used,unpin it immediately
    // cerr<<"InsertTuple3\n";
    if(sign) return true;
    else //insert fail
    {
        // cerr<<"InsertTuple4\n";
        if(first->GetNextPageId()==INVALID_PAGE_ID)//heap is full,so add a new page
        {
            // cerr<<"InsertTuple5\n";
            page_id_t new_id;
            TablePage* new_page;
            new_page=reinterpret_cast<TablePage*> (buffer_pool_manager_->NewPage(new_id));
            if(new_page==nullptr) return false;//newpage failed
            first->SetNextPageId(new_id);
            new_page->WLatch();
            new_page->Init(new_id,first->GetPageId(),log_manager_,txn);
            sign=new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);//insert to the new page
            new_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(new_page->GetPageId(),sign);
            now_insert_page_id=new_id;
            return sign;
        }
    }
}


bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
    TablePage* old_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    bool isNotEnough=false,sign;
    Row* old_row=new Row(rid);
    old_page->WLatch();
    sign=old_page->UpdateTuple(row,old_row,schema_,txn,lock_manager_,log_manager_,isNotEnough);//just need old row rid
    old_page->WUnlatch();
    if(!(!sign && isNotEnough)) buffer_pool_manager_->UnpinPage(old_page->GetPageId(),sign);//if not used below,unpin here
    if(!sign && !isNotEnough) return false;
    if(sign) 
    {
        row.SetRowId(rid);//if update success,RowId should be same as the old
        return true;
    }
    if(!sign && isNotEnough)//if space is not enough,new a page P.Then insert the new row into the P and delete old row from old page
    {
        old_page->WLatch();
        old_page->ApplyDelete(rid,txn,log_manager_);
        old_page->WUnlatch();
        page_id_t new_page_id;
        TablePage* new_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));
        new_page->Init(new_page_id,old_page->GetPageId(),log_manager_,txn);//first initilize
        new_page->SetNextPageId(old_page->GetNextPageId());
        old_page->SetNextPageId(new_page_id);//update
        new_page->WLatch();
        sign=new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
        new_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(new_page->GetPageId(),sign);
        buffer_pool_manager_->UnpinPage(old_page->GetPageId(),true);
        return sign;
    }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
    TablePage* now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    now_page->WLatch();
    now_page->ApplyDelete(rid,txn,log_manager_);
    now_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(now_page->GetPageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
    bool sign;
    TablePage* now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    now_page->RLatch();
    sign=now_page->GetTuple(row,schema_,txn,lock_manager_);
    now_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(now_page->GetPageId(),false);
    return sign;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
    TablePage* page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
    RowId rid;
    page->RLatch();
    if(!page->GetFirstTupleRid(&rid))
    {page->RUnlatch();return End();}
    Row row(rid);
    page->GetTuple(&row,schema_,txn,lock_manager_);//get the content of this row
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return TableIterator(page->GetPageId(),row,buffer_pool_manager_,this);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    /*page_id_t page_id=first_page_id_;
    TablePage* page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
    TablePage* first_page=page;
    while(page->GetNextPageId()!=INVALID_PAGE_ID)
    {
        buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));//to the final page
    }
    page->RLatch();
    RowId* rid=new RowId,*next_rid=new RowId;
    page->RLatch();
    page->GetFirstTupleRid(rid);
    while(page->GetNextTupleRid(*rid,next_rid)) 
      *rid=*next_rid;//to the final row
    rid->Set(page->GetPageId()+1,0);//end() is the first data to go out of range
    Row row(*rid);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);*/
    Row row;
    return TableIterator(INVALID_PAGE_ID,row,buffer_pool_manager_,this);
}


// #include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
// bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
// uint32_t total_length = row.GetSerializedSize(this->schema_);
//   if(total_length + 32 > PAGE_SIZE) // larger than one page size
//     return false;
//   auto current_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//   if(current_page == nullptr)
//     return false;
//   current_page->WLatch(); // 
//   while(current_page->InsertTuple(row, this->schema_, txn, lock_manager_, log_manager_) == false) 
//   { // fail to insert due to not enough space
//     auto next_page_id = current_page->GetNextPageId();
//     if(next_page_id != INVALID_PAGE_ID) 
//     { // valid next page
//       current_page->WUnlatch();
//       buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
//       current_page = static_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
//       current_page->WLatch();
//     }
//     else 
//     { // create new page
//       auto new_page = static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
//       if(new_page == nullptr) 
//       {
//         current_page->WUnlatch();
//         buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
//         return false;
//       }
//       new_page->WLatch();
//       current_page->SetNextPageId(next_page_id);
//       new_page->Init(next_page_id, current_page->GetPageId(), log_manager_, txn);
//       current_page->WUnlatch();
//       buffer_pool_manager_->UnpinPage(current_page->GetPageId(), true);
//       current_page = new_page;
//     }
//   }
//   current_page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(current_page->GetPageId(), true);
//   return true;

//     TablePage* first=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
//     bool sign=false;
//     cerr<<"InsertTuple1\n";
//     while(1)
//     {
//         cerr<<"InsertTuple2\n";
//         first->WLatch();
//         cerr<<"InsertTuple2.2\n";
//         sign=first->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
//         cerr<<"InsertTuple2.5\n";
//         first->WUnlatch();
//         cerr<<"InsertTuple2.8\n";
//         buffer_pool_manager_->UnpinPage(first->GetPageId(),sign);//after used,unpin it immediately
//         cerr<<"InsertTuple3\n";
//         if(sign) return true;
//         else //insert fail
//         {
//             cerr<<"InsertTuple4\n";
//             if(first->GetNextPageId()==INVALID_PAGE_ID)//heap is full,so add a new page
//             {
//                 cerr<<"InsertTuple5\n";
//                 page_id_t new_id;
//                 TablePage* new_page;
//                 new_page=reinterpret_cast<TablePage*> (buffer_pool_manager_->NewPage(new_id));
//                 if(new_page==nullptr) return false;//newpage failed
//                 first->SetNextPageId(new_id);
//                 new_page->WLatch();
//                 new_page->Init(new_id,first->GetPageId(),log_manager_,txn);
//                 sign=new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);//insert to the new page
//                 new_page->WUnlatch();
//                 buffer_pool_manager_->UnpinPage(new_page->GetPageId(),sign);
//                 return sign;
//             }
//             cerr<<"InsertTuple6\n";
//             first=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first->GetNextPageId()));//to next page
//             cerr<<"InsertTuple7\n";
//         }
//     }
// }

// bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
//   // Find the page which contains the tuple.
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   // If the page could not be found, then abort the transaction.
//   if (page == nullptr) {
//     return false;
//   }
//   // Otherwise, mark the tuple as deleted.
//   page->WLatch();
//   page->MarkDelete(rid, txn, lock_manager_, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//   return true;
// }

// /**
//  * TODO: Student Implement
//  */
// bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
//     TablePage* old_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//     bool isNotEnough=false,sign;
//     Row* old_row=new Row(rid);
//     old_page->WLatch();
//     sign=old_page->UpdateTuple(row,old_row,schema_,txn,lock_manager_,log_manager_,isNotEnough);//just need old row rid
//     old_page->WUnlatch();
//     if(!(!sign && isNotEnough)) buffer_pool_manager_->UnpinPage(old_page->GetPageId(),sign);//if not used below,unpin here
//     if(!sign && !isNotEnough) return false;
//     if(sign) 
//     {
//         row.SetRowId(rid);//if update success,RowId should be same as the old
//         return true;
//     }
//     if(!sign && isNotEnough)//if space is not enough,new a page P.Then insert the new row into the P and delete old row from old page
//     {
//         old_page->WLatch();
//         old_page->ApplyDelete(rid,txn,log_manager_);
//         old_page->WUnlatch();
//         page_id_t new_page_id;
//         TablePage* new_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));
//         new_page->Init(new_page_id,old_page->GetPageId(),log_manager_,txn);//first initilize
//         new_page->SetNextPageId(old_page->GetNextPageId());
//         old_page->SetNextPageId(new_page_id);//update
//         new_page->WLatch();
//         sign=new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
//         new_page->WUnlatch();
//         buffer_pool_manager_->UnpinPage(new_page->GetPageId(),sign);
//         buffer_pool_manager_->UnpinPage(old_page->GetPageId(),true);
//         return sign;
//     }
// }

// /**
//  * TODO: Student Implement
//  */
// void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
//     TablePage* now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//     now_page->WLatch();
//     now_page->ApplyDelete(rid,txn,log_manager_);
//     now_page->WUnlatch();
//     buffer_pool_manager_->UnpinPage(now_page->GetPageId(),true);
// }

// void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
//   // Find the page which contains the tuple.
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   assert(page != nullptr);
//   // Rollback to delete.
//   page->WLatch();
//   page->RollbackDelete(rid, txn, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
// }

// /**
//  * TODO: Student Implement
//  */
// bool TableHeap::GetTuple(Row *row, Transaction *txn) {
//     bool sign;
//     TablePage* now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
//     now_page->RLatch();
//     sign=now_page->GetTuple(row,schema_,txn,lock_manager_);
//     now_page->RUnlatch();
//     buffer_pool_manager_->UnpinPage(now_page->GetPageId(),false);
//     return sign;
// }

// void TableHeap::DeleteTable(page_id_t page_id) {
//   if (page_id != INVALID_PAGE_ID) {
//     auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
//     if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
//       DeleteTable(temp_table_page->GetNextPageId());
//     buffer_pool_manager_->UnpinPage(page_id, false);
//     buffer_pool_manager_->DeletePage(page_id);
//   } else {
//     DeleteTable(first_page_id_);
//   }
// }

// /**
//  * TODO: Student Implement
//  */
// TableIterator TableHeap::Begin(Transaction *txn) {
//     TablePage* page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
//     RowId rid;
//     page->RLatch();
//     if(!page->GetFirstTupleRid(&rid))
//     {page->RUnlatch();return End();}
//     Row row(rid);
//     page->GetTuple(&row,schema_,txn,lock_manager_);//get the content of this row
//     page->RUnlatch();
//     buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
//     return TableIterator(page->GetPageId(),row,buffer_pool_manager_,this);
// }

// /**
//  * TODO: Student Implement
//  */
// TableIterator TableHeap::End() {
//     /*page_id_t page_id=first_page_id_;
//     TablePage* page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
//     TablePage* first_page=page;
//     while(page->GetNextPageId()!=INVALID_PAGE_ID)
//     {
//         buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
//         page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));//to the final page
//     }
//     page->RLatch();
//     RowId* rid=new RowId,*next_rid=new RowId;
//     page->RLatch();
//     page->GetFirstTupleRid(rid);
//     while(page->GetNextTupleRid(*rid,next_rid)) 
//       *rid=*next_rid;//to the final row
//     rid->Set(page->GetPageId()+1,0);//end() is the first data to go out of range
//     Row row(*rid);
//     page->RUnlatch();
//     buffer_pool_manager_->UnpinPage(page->GetPageId(),false);*/
//     Row row;
//     return TableIterator(INVALID_PAGE_ID,row,buffer_pool_manager_,this);
// }

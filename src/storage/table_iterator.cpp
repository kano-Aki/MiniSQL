#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(const TableIterator &other) {
    now_page_id=other.getNowPageId();
    now_row=other.getNowRow();
    buffer_pool_manager_=other.getManager();
}


bool TableIterator::operator==(const TableIterator &itr) const {
    if(now_row.GetRowId()==itr.getNowRow().GetRowId()) return true;
    else return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
    if(now_row.GetRowId()==itr.getNowRow().GetRowId()) return false;
    else return true;
}

const Row &TableIterator::operator*() {
    return now_row;
}

Row *TableIterator::operator->() {
    return &now_row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    //memcpy(now_page,itr.getNowPage(),sizeof(TablePage));//copy the content of pointer
    now_page_id=itr.getNowPageId();
    now_row=itr.getNowRow();
    buffer_pool_manager_=itr.getManager();
    table_heap_=itr.getTableHeap();
}

// ++iter
TableIterator &TableIterator::operator++() {
    RowId* temp=new RowId();
    TablePage* now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(now_page_id));
    now_page->RLatch();
    bool sign=now_page->GetNextTupleRid(now_row.GetRowId(),temp);
    now_page->RUnlatch();
    if(sign)//当前page还有元组
    {
        Row new_row(*temp);
        now_page->RLatch();
        now_page->GetTuple(&new_row,table_heap_->schema_,nullptr,table_heap_->lock_manager_);//read the data of now_row
        now_page->RUnlatch();
        now_row=new_row;
        buffer_pool_manager_->UnpinPage(now_page_id,false);
        return *this;
    }
    else
    {
        now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(now_page->GetNextPageId()));
        if(now_page==nullptr)//当前为最后一个page
        {
            temp->Set(INVALID_PAGE_ID,0);//设置为end
            Row new_row(*temp);
            now_row=new_row;
            now_page_id=INVALID_PAGE_ID;
            return *this;
        }
        else
        {
            now_page->RLatch();
            now_page_id=now_page->GetPageId();
            now_page->GetFirstTupleRid(temp);
            Row new_row(*temp);
            now_page->GetTuple(&new_row,table_heap_->schema_,nullptr,table_heap_->lock_manager_);//read the data of now_row
            now_page->RUnlatch();
            now_row=new_row;
            buffer_pool_manager_->UnpinPage(now_page_id,false);
            return *this;
        }
    }
}

// iter++
TableIterator TableIterator::operator++(int) {
    TableIterator before(*this);
    RowId* temp=new RowId();
    TablePage* now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(now_page_id));
    // cerr<<"TI++1\n";
    now_page->RLatch();
    bool sign=now_page->GetNextTupleRid(now_row.GetRowId(),temp);
    // cerr<<"TI++4\n";
    now_page->RUnlatch();
    if(sign)//当前page还有元组
    {
        Row new_row(*temp);
    // cerr<<"TI++2\n";
        now_page->RLatch();
        now_page->GetTuple(&new_row,table_heap_->schema_,nullptr,table_heap_->lock_manager_);//read the data of now_row
    // cerr<<"TI++5\n";
        now_page->RUnlatch();
        now_row=new_row;
        buffer_pool_manager_->UnpinPage(now_page_id,false);
        return TableIterator(before);
    }
    else
    {
        now_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(now_page->GetNextPageId()));
        if(now_page==nullptr)//当前为最后一个page
        {
            temp->Set(INVALID_PAGE_ID,0);//设置为end
            Row new_row(*temp);
            now_row=new_row;
            now_page_id=INVALID_PAGE_ID;
            return TableIterator(before);
        }
        else
        {
    // cerr<<"TI++3\n";
            now_page->RLatch();
            now_page_id=now_page->GetPageId();
            now_page->GetFirstTupleRid(temp);
            Row new_row(*temp);
            now_page->GetTuple(&new_row,table_heap_->schema_,nullptr,table_heap_->lock_manager_);//read the data of now_row
    // cerr<<"TI++6\n";
            now_page->RUnlatch();
            now_row=new_row;
            buffer_pool_manager_->UnpinPage(now_page_id,false);
            return TableIterator(before);
        }
    }
}

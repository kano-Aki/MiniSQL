//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    // cerr<<"InsertExecutor Next\n";
    Row ins_row;
    RowId ins_rid;
    bool sign=child_executor_->Next(&ins_row,&ins_rid);//get value
    // cerr<<"InsertExecutor Next ins_row "<<ins_row.GetRowId().GetPageId()<<' '<<ins_row.GetRowId().GetSlotNum()<<'\n';
    // cerr<<"InsertExecutor Next ins_rid "<<ins_rid.GetPageId()<<' '<<ins_rid.GetSlotNum()<<'\n';
    if(!sign) return false;

    auto schema=table_info->GetSchema();
    auto columns_=schema->GetColumns();
    std::vector<IndexInfo *> indexs_;
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexs_);
    uint32_t col_index=0;
    // cerr<<"InsertExecutor Next columns_.size()="<<columns_.size()<<'\n';
    for(auto column:columns_)
    {
        if(column->IsUnique())
        {
            // cerr<<"InsertExecutor Next Found Unique "<<col_index<<"\n";
            for(auto index_info:indexs_)
            {
                // cerr<<"InsertExecutor Next Found index\n";
                IndexInfo* temp=nullptr;
                bool sign=false;
                for(auto it:index_info->GetKeyMapping())//find whether has that column
                {
                    // cerr<<"InsertExecutor Next index info it="<<it<<'\n';
                    if(it==col_index)
                    {
                        temp=index_info;
                        break;
                    }
                }
                if(temp==nullptr) continue;//no index in this column
                // cerr<<"InsertExecutor Next Found index for this column\n";
                Row ind_row;
                ins_row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_row);//得到查询用的row
                std::vector<RowId> result;
                // cerr<<"InsertExecutor Next ind_row "<<(long long)ind_row.GetFieldCount()<<'\n';
                temp->GetIndex()->ScanKey(ind_row,result,exec_ctx_->GetTransaction());
                // cerr<<"InsertExecutor Next result "<<result.size()<<'\n';
                if(!result.empty()) return false;//the inserted value has been in table
            }
        }
        col_index++;
    }
    // cerr<<"InsertExecutor Next Cols Finish\n";
    if(!table_info->GetTableHeap()->InsertTuple(ins_row,exec_ctx_->GetTransaction())) return false;
    // cerr<<"InsertExecutor Next THInsert Finish\n";
    for(auto index_info:indexs_)
    {
        // cerr<<"InsertExecutor Next Insert into "<<index_info->GetIndexName()<<"\n";
        Row ind_row;
        ins_row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_row);//得到查询用的row
        // cerr<<"InsertExecutor Next Insert into index "<<index_info->GetIndexName()<<"\n";
        index_info->GetIndex()->InsertEntry(ind_row,ins_row.GetRowId(),exec_ctx_->GetTransaction());
    }
    // cerr<<"InsertExecutor Next All Done\n";
    return true;
}
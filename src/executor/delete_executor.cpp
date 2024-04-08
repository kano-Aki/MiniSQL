//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    child_executor_->Init();
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    // cerr<<"DeleteExecutor Next\n";
    Row del_row;
    RowId del_rid;
    bool sign=child_executor_->Next(&del_row,&del_rid);
    if(!sign) return false;
    table_info->GetTableHeap()->ApplyDelete(del_rid,exec_ctx_->GetTransaction());
    std::vector<IndexInfo *> indexs_;
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexs_);
    for(auto index_info:indexs_)
    {
        // cerr<<"DeleteExecutor Next in loop1 "<<index_info->GetIndexName()<<"\n";
        Row ind_row(del_rid);
        del_row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_row);//得到查询用的row
        // cerr<<"DeleteExecutor Next in loop2\n";
        index_info->GetIndex()->RemoveEntry(ind_row,del_rid,exec_ctx_->GetTransaction());
        // cerr<<"remove"<<"\n";
    }
    return true;
}
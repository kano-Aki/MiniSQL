//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/delete_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
    child_executor_->Init();
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),index_info_);
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    Row upd_row;
    RowId upd_rid;
    bool sign=child_executor_->Next(&upd_row,&upd_rid);
    if(!sign) return false;
    Row new_row=GenerateUpdatedTuple(upd_row);//get new row

    auto schema=GetOutputSchema();//check unique
    auto columns_=schema->GetColumns();
    for(auto upd_col_index:plan_->GetUpdateAttr())//traverse update column
    {
        if(columns_[upd_col_index.first]->IsUnique())
        {
            for(auto index_info:index_info_)//check all the index which has this unique column
            {
                IndexInfo* temp=nullptr;
                bool sign=false;
                for(auto it:index_info->GetKeyMapping())
                {
                    if(it==upd_col_index.first)
                    {
                        temp=index_info;
                        break;
                    }
                }
                if(temp==nullptr) continue;//no index in this column
                std::vector<RowId> result;
                Row ind_row;
                new_row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_row);
                ind_row.SetRowId(new_row.GetRowId());
                // cerr<<"UpdateExecutorN indrow "<<ind_row.GetFieldCount()<<'\n';
                temp->GetIndex()->ScanKey(ind_row,result,exec_ctx_->GetTransaction());
                if(!result.empty()) return false;//the update value has been in table
            }
        }
    }

    table_info->GetTableHeap()->UpdateTuple(new_row,upd_rid,exec_ctx_->GetTransaction());//update

    for(auto index_info:index_info_)//only Single-column indexes,update index
    {
        for(auto it:index_info->GetKeyMapping())
        {
            if(plan_->GetUpdateAttr().find(it)!=plan_->GetUpdateAttr().end())//just judge whether index column is in update
            {
                Row ind_del_row,ind_ins_row;
                upd_row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_del_row);
                new_row.GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_ins_row);
                // cerr<<"UpdateExecutorN ind_del_row "<<ind_del_row.GetFieldCount()<<'\n';
                index_info->GetIndex()->RemoveEntry(ind_del_row,upd_rid,exec_ctx_->GetTransaction());
                // cerr<<"UpdateExecutorN ind_ins_row "<<ind_ins_row.GetFieldCount()<<'\n';
                index_info->GetIndex()->InsertEntry(ind_ins_row,new_row.GetRowId(),exec_ctx_->GetTransaction());
                break;//next index
            }
        }
    }
    return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
    std::vector<Field> fields;
    for(size_t i=0;i<src_row.GetFieldCount();i++)
    {
        auto upd=plan_->GetUpdateAttr().find(i);
        if(upd!=plan_->GetUpdateAttr().end())
        {
            Field new_field(upd->second->Evaluate(nullptr));
            fields.push_back(new_field);
        }
        else
        {
            Field new_field(*(src_row.GetField(i)));
            fields.push_back(new_field);
        }
    }
    return Row(fields);
}
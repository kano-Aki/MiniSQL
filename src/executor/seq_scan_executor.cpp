//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
    // cerr<<"SeqScanExecutor Init\n";
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
    itr=table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
    // cerr<<"SeqScanExecutor Next\n";
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
    AbstractExpressionRef pred=plan_->GetPredicate();

    while(1)
    {
        if(itr==table_info->GetTableHeap()->End()) 
            return false;
        *row=*itr;
        *rid=itr->GetRowId();
        ++itr;
        std::vector<uint32_t> col;
        Row pro_row(*rid);
        row->GetKeyFromRow(table_info->GetSchema(),plan_->OutputSchema(),pro_row);//投影
        *row=pro_row;

        if(pred==nullptr) return true;//no predicate
        else
        {
            Field res=pred->Evaluate(row);
            if(res.CompareEquals(Field(kTypeInt, 1))==kTrue) return true;
        }
    }
}

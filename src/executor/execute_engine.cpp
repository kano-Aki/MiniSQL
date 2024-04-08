#include "executor/execute_engine.h"
#include <set>

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

#include <cstdio>
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"
extern "C" {
int yyparse(void);
// FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine(int flag) {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  if(flag)
  {return;}
  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.*/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   /**/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    // cerr<<"ExecutePlan After Init\n";
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
    // cerr<<"ExecutePlan After Next\n";
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  // cerr<<"Plan the query.\n";
  // cerr<<current_db_<<'\n';
  if(current_db_.empty())
  {
    cout<<"No current using database!\n";
    return DB_FAILED;
  }
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);
  // cerr<<"ExecutePlan Finish.\n";

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  std::string name=ast->child_->val_;
  DBStorageEngine* db=new DBStorageEngine(name);
  if(db==nullptr)
  return DB_FAILED;
  dbs_[name]=db;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  std::string name =ast->child_->val_;
  if(dbs_.find(name)==dbs_.end())
  return DB_FAILED;
  if(name==current_db_)
  current_db_.clear();
  auto db=dbs_[name];
  db->~DBStorageEngine();
  dbs_.erase(name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  cout<<"+--------------------------+\n";
  cout<<"|Showing Databases:\n";
  cout<<"+--------------------------+\n";
  for(auto [x,y]:dbs_)
  cout<<"|"<<x<<'\n';
  cout<<"+--------------------------+\n";
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  std::string name=ast->child_->val_;
  if(dbs_.find(name)==dbs_.end())
  {cout<<"Database not existent.\n";return DB_FAILED;}
  current_db_=name;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(current_db_.empty())
  {cout<<"No current using database!\n";return DB_FAILED;}
  cout<<"+-----------------------------------------+\n";
  cout<<"|Showing tables in "<<current_db_<<'\n';
  cout<<"+-----------------------------------------+\n";
  std::vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto x:tables)
  cout<<"|"<<x->GetTableName()<<'\n';
  cout<<"+-----------------------------------------+\n";
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif

  if(current_db_.empty())
  {cout<<"No current using database!\n";return DB_FAILED;}
  std::string name=ast->child_->val_;
  auto now=ast->child_->next_->child_;
  std::vector<Column*> cols;
  std::set<std::string> uniques;
  std::set<std::string> primarys;
  // cerr<<"ExecuteCreateTable:begin loop\n";
  for(int i=0;now!=nullptr;now=now->next_,i++)
  if(now->type_==kNodeColumnDefinition)
  {
    std::string name=now->child_->val_,typestring=now->child_->next_->val_;
    // cerr<<"ExecuteCreateTable: loop "<<i<<'\n';
    bool unique=now->val_!=nullptr&&std::string(now->val_)=="unique";
    if(unique)
    uniques.insert(name);//cerr<<"CT unique "<<name<<'\n';
    TypeId type; 
    Column* col;
    if(typestring=="int")
    {
      type=kTypeInt;
      col=new Column(name,type,i,0,unique);
    }
    else
    if(typestring=="float")
    {
      type=kTypeFloat;
      col=new Column(name,type,i,0,unique);
    }
    else
    if(typestring=="char")
    {
      type=kTypeChar;
      std::string length=now->child_->next_->child_->val_;
      int len=stoi(length);
      if(len<=0||length.find('.')!=length.npos)
      {
        return DB_FAILED;
      }
      col=new Column(name,type,len,i,0,unique);
    }
    else
    return DB_FAILED;
    cols.push_back(col);
  }
  else
  if(now->type_==kNodeColumnList)
  {
    // cerr<<"ExecuteCreateTable: loop "<<i<<'\n';
    for(auto p=now->child_;p!=nullptr;p=p->next_)
    primarys.insert(p->val_);
    // cerr<<"ExecuteCreateTable: loop "<<i<<'\n';
  }
  for(auto x:cols)
  {
    if(primarys.find(x->GetName())!=primarys.end())
    x->SetUnique(1),uniques.insert(x->GetName());
  }
  Schema* schema=new Schema(cols);
  TableInfo* table=nullptr;
  auto mng=context->GetCatalog();
  // cerr<<"are?\n";
  // dbs_[current_db_]->catalog_mgr_->CreateTable(name,schema,nullptr,table);
  mng->CreateTable(name,schema,context->GetTransaction(),table);
  for(auto x:uniques)
  {
    IndexInfo* index=nullptr;
    mng->CreateIndex(name,"IndexForUniqueKey_"+x,{x},context->GetTransaction(),index,"bptree");
  }
  // cerr<<"soga.\n";
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
 std::string name=ast->child_->val_;
  if(current_db_.empty())
  {cout<<"No current using database!\n";return DB_FAILED;}
 auto mng=context->GetCatalog();
 return mng->DropTable(name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty())
  {cout<<"No current using database!\n";return DB_FAILED;}
  cout<<"+----------------------------------------------------+\n";
  cout<<"|Showing indices in database "<<current_db_<<'\n';
  std::vector<TableInfo*> tables;
  auto mng=context->GetCatalog();
  mng->GetTables(tables);
  for(auto x:tables)
  {
    cout<<"+----------------------------------------------------+\n";
    cout<<"|Showing indices in table "<<x->GetTableName()<<'\n';
    std::vector<IndexInfo*> indices;
    mng->GetTableIndexes(x->GetTableName(),indices);
    for(auto y:indices)
    cout<<"|----"<<y->GetIndexName()<<'\n';
  }
  cout<<"+----------------------------------------------------+\n";
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty())
  {cout<<"No current using database!\n";return DB_FAILED;}
  auto mng=context->GetCatalog();
  std::string name=ast->child_->val_,table=ast->child_->next_->val_;
  std::vector<std::string> keys;
  std::string type="btree";
  for(auto now=ast->child_->next_->next_;now!=nullptr;now=now->next_)
  {
    if(now->type_==kNodeColumnList)
    {
      for(auto pcol=now->child_;pcol!=nullptr;pcol=pcol->next_)
      {
        keys.push_back(pcol->val_);
      }
    }
    if(std::string(now->val_)=="index type")
    {
      type=now->child_->val_;
    }
  }
  // cerr<<"ExecuteCreateIndex\n";
  IndexInfo* index=nullptr;
  mng->CreateIndex(table,name,keys,context->GetTransaction(),index,type);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty())
  return DB_FAILED;
  auto mng=context->GetCatalog();
  std::string name=ast->child_->val_;
  std::vector<TableInfo*> tables;
  mng->GetTables(tables);
  for(auto x:tables)
  {
    IndexInfo* index;
    mng->GetIndex(x->GetTableName(),name,index);
    if(index!=nullptr)
    {
      mng->DropIndex(x->GetTableName(),name);
    }
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  std::ifstream ist;
  std::string name=ast->child_->val_;
  ist.open(name);
  const int maxsize=1024;
  char cmd[maxsize];
  // for print syntax tree
  // TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
  uint32_t syntax_tree_id = 0;
  clock_t Ts=clock();
  for(int i=1;ist.getline(cmd,maxsize);i++)
  {
    cout<<name<<" line "<<i<<": "<<cmd<<'\n';

    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      // Comment them out if you don't need to debug the syntax tree
      printf("[INFO] Sql syntax parse ok!\n");
      // SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      // printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
    }

    auto result = this->Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
  }
  clock_t Tt=clock();
  cout<<"Query OK, "<< "(" <<(double)(Tt-Ts)/CLOCKS_PER_SEC<<"sec)"<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}

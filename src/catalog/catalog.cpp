#include "catalog/catalog.h"

static uint32_t crc_table[256];

static unsigned long bitrev(unsigned long input, int bw)
{
	int i;unsigned long var=0;
	for(i=0;i<bw;i++)
	{
		if(input & 0x01)
		{
			var |= 1<<(bw-1-i);
		}
		input>>=1;
	}
	return var;
}
void crc32_init(unsigned long poly)
{
	int i,j;unsigned long c;
	poly=bitrev(poly,32);
	for(i=0; i<256; i++)
	{
		c = i;
		for (j=0; j<8; j++)
			if(c&1)
				c=poly^(c>>1);
			else
				c=c>>1;
		crc_table[i] = c;
	}
}
unsigned long crc32(unsigned long crc, void* input, int len)
{
	int i;unsigned char index;unsigned char* pch;
	pch = (unsigned char*)input;
	for(i=0;i<len;i++)
	{
		index = (unsigned char)(crc^*pch);
		crc = (crc>>8)^crc_table[index];
		pch++;
	}
	return crc;
}

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    void *src=(uint32_t*)malloc((GetSerializedSize()));
    memcpy(src,buf-(GetSerializedSize()),(GetSerializedSize()));
    crc32_init(0x4C11DB7);
    uint32_t crc = 0xFFFFFFFF;
	crc=crc32(crc,src,10);
	crc ^= 0xFFFFFFFF;
    MACH_WRITE_UINT32(buf, crc);
    buf += 4;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    void *src=(uint32_t*)malloc(sizeof(uint32_t)*(3+table_nums*2+index_nums*2));
    memcpy(src,buf-sizeof(uint32_t)*(3+table_nums*2+index_nums*2),sizeof(uint32_t)*(3+table_nums*2+index_nums*2));
    crc32_init(0x4C11DB7);
    uint32_t crc = 0xFFFFFFFF;
	crc=crc32(crc,src,10);
	crc ^= 0xFFFFFFFF;
    uint32_t old_crc=MACH_READ_UINT32(buf);
    buf += 4;
    assert(crc==old_crc);
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
    return sizeof(uint32_t)*(3+table_meta_pages_.size()*2+index_meta_pages_.size()*2);
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
      if(init)
      {
        // cerr<<"What\n";
          catalog_meta_=new CatalogMeta;
          catalog_meta_->table_meta_pages_.clear();
          catalog_meta_->index_meta_pages_.clear();//initilize catalog meta
          TablePage* meta_page=reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID));
          meta_page->WLatch();
          catalog_meta_->SerializeTo(meta_page->GetData());//write to the catalog meta page
          meta_page->WUnlatch();

          next_table_id_=catalog_meta_->GetNextTableId();
          next_index_id_=catalog_meta_->GetNextIndexId();
          table_names_.clear();
          tables_.clear();
          index_names_.clear();
          indexes_.clear();
          buffer_pool_manager_=buffer_pool_manager;
          lock_manager_=lock_manager;
          log_manager_=log_manager;//initilize the member of CatalogManager
          buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
      }
      else 
      {
        // cerr<<"How\n";
          buffer_pool_manager_=buffer_pool_manager;
          lock_manager_=lock_manager;
          log_manager_=log_manager;
          TablePage* meta_page=reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID));
          meta_page->RLatch();
          catalog_meta_=catalog_meta_->DeserializeFrom(meta_page->GetData());//get the catalog meta data
          meta_page->RUnlatch();
          next_table_id_=catalog_meta_->GetNextTableId();
          next_index_id_=catalog_meta_->GetNextIndexId();
          buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
          for(auto table:catalog_meta_->table_meta_pages_)//build the table info
          {
            // cerr<<"What happens\n";
              table_id_t table_id=table.first;
              page_id_t page_id=table.second;

              TablePage* table_info_page=reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(page_id));
              TableMetadata* meta_data=nullptr;
              table_info_page->RLatch();
            //   cerr<<"Whether\n";
              meta_data->DeserializeFrom(table_info_page->GetData(),meta_data);//get the table meta data
              table_info_page->RUnlatch();
                // cerr<<"Which\n";
              std::string table_name=meta_data->GetTableName();
            //   cerr<<table_name<<'\n';
              TableInfo* table_info=TableInfo::Create();
              TableHeap* table_heap=TableHeap::Create(buffer_pool_manager,meta_data->GetFirstPageId(),meta_data->GetSchema(),log_manager_,lock_manager_);
              table_info->Init(meta_data,table_heap);//get the table info
              TablePage* data_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(table_heap->GetFirstPageId()));
              for(TableIterator it=table_heap->Begin(nullptr);it!=table_heap->End();it++)
              {
                Row tmp=*it;
                table_heap->InsertTuple(tmp,nullptr);
                // cerr<<"ga\n";
              }
              
              table_names_.emplace(std::unordered_map<std::string, table_id_t>::value_type(table_name,table_id));
              tables_.emplace(std::unordered_map<table_id_t, TableInfo *>::value_type(table_id,table_info));
              buffer_pool_manager->UnpinPage(page_id,false);
          }
          for(auto index:catalog_meta_->index_meta_pages_)
          {            
              index_id_t index_id=index.first;
              page_id_t page_id=index.second;

              TablePage* index_info_page=reinterpret_cast<TablePage*>(buffer_pool_manager->FetchPage(page_id));
              std::vector<uint32_t> key_map;
              key_map.clear();
              IndexMetadata* meta_data=nullptr;
              index_info_page->RLatch();
              meta_data->DeserializeFrom(index_info_page->GetData(),meta_data);//get the index meta data
              index_info_page->RUnlatch();

              std::string index_name=meta_data->GetIndexName();
              std::string table_name=tables_.find(meta_data->GetTableId())->second->GetTableName();
              auto it=index_names_.find(table_name);
              if(it==index_names_.end())//now no this table
              {
                  std::unordered_map<std::string, index_id_t> temp;
                  temp.clear();
                  temp.emplace(std::unordered_map<std::string, index_id_t>::value_type(index_name,index_id));
                  index_names_.emplace(std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::value_type(table_name,temp));
              }
              else it->second.emplace(std::unordered_map<std::string, index_id_t>::value_type(index_name,index_id));
              
              IndexInfo* index_info=IndexInfo::Create();

              TableInfo* table_info=(tables_.find(meta_data->GetTableId()))->second;

              index_info->Init(meta_data,tables_.find(meta_data->GetTableId())->second,buffer_pool_manager_);//get the index info
              for(auto it=table_info->GetTableHeap()->Begin(nullptr);it!=table_info->GetTableHeap()->End();it++)
              {
                    Row ind_row;
                    it->GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_row);
                    index_info->GetIndex()->InsertEntry(ind_row,it->GetRowId(),nullptr);
              }
              indexes_.emplace(std::unordered_map<index_id_t, IndexInfo *>::value_type(index_id,index_info));
              buffer_pool_manager->UnpinPage(page_id,false);
          }
      }
    //   cerr<<"Where\n";
}

CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test*/
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
* TODO: Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) 
{
    
    auto it=table_names_.find(table_name);
    if(it!=table_names_.end()) return DB_TABLE_ALREADY_EXIST;
    auto copy_schema=Schema::DeepCopySchema(schema);

    page_id_t new_page_id;                                  
    TablePage* new_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));//get a new page for the data of table
    if(new_page==nullptr) return DB_FAILED;
    new_page->WLatch();
    new_page->Init(new_page_id,INVALID_PAGE_ID,log_manager_,txn);//initialize the first data page
    new_page->WUnlatch();
    page_id_t next=new_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(new_page_id,true);

    TableHeap* new_heap=TableHeap::Create(buffer_pool_manager_,new_page_id,copy_schema,log_manager_,lock_manager_);//new a heap for this table
    TableMetadata* new_meta_data=TableMetadata::Create(next_table_id_,table_name,new_page_id,copy_schema);//create the table meta data
    if(table_info==nullptr) table_info=TableInfo::Create();
    table_info->Init(new_meta_data,new_heap);//create the table info 

    page_id_t meta_page_id;
    TablePage* meta_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(meta_page_id));//get a new page for the metadata of table
    if(meta_page==nullptr) return DB_FAILED;
    meta_page->WLatch();
    new_meta_data->SerializeTo(meta_page->GetData());//write the table meta data to the page
    meta_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(meta_page_id,true);

    (catalog_meta_->table_meta_pages_)[next_table_id_]=meta_page_id;
    table_names_.insert(std::unordered_map<std::string, table_id_t>::value_type(table_name,next_table_id_));
    tables_.insert(std::unordered_map<table_id_t, TableInfo *>::value_type(next_table_id_,table_info));
    next_table_id_++;//update catalog metadata
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
    auto name=table_names_.find(table_name);
    if(name==table_names_.end()) return DB_TABLE_NOT_EXIST;
    auto it=tables_.find(name->second);
    if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
    table_info=it->second;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
    for(table_id_t i=0;i<next_table_id_;i++)
    {
        auto it=tables_.find(i);
        if(it!=tables_.end()) tables.push_back(it->second);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) 
{
    auto it_table=table_names_.find(table_name);
    if(it_table==table_names_.end()) return DB_TABLE_NOT_EXIST;
    auto table_index=index_names_.find(table_name);
    if(table_index!=index_names_.end())
    {
        auto it=table_index->second.find(index_name);
        if(it!=table_index->second.end()) return DB_INDEX_ALREADY_EXIST;
    }
// cerr<<"dodo\n";
    page_id_t meta_page_id;                                  
    TablePage* meta_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(meta_page_id));//get a new page for the meta data of index
    if(meta_page==nullptr) return DB_FAILED;
    table_id_t table_id=table_names_[table_name];
    TableInfo* table_info=tables_[table_id];
    std::vector<uint32_t> key_map;
    for(auto key_name:index_keys)
    {
        uint32_t key_index; 
        if(table_info->GetSchema()->GetColumnIndex(key_name,key_index)==DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
        key_map.push_back(key_index);
    }
    IndexMetadata* meta_data=IndexMetadata::Create(next_index_id_,index_name,table_id,key_map);
    // cerr<<"CreateIndex meta_page_id="<<meta_page_id<<'\n';
    meta_page->WLatch();
    meta_data->SerializeTo(meta_page->GetData());//write the metadata to the page
    meta_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(meta_page_id,true);
// cerr<<"rere\n";
    if(index_info==nullptr) index_info=IndexInfo::Create();
    index_info->Init(meta_data,table_info,buffer_pool_manager_);//build the index info
    // cerr<<"fafa\n";
    int count=0;
    for(auto it=table_info->GetTableHeap()->Begin(nullptr);it!=table_info->GetTableHeap()->End();it++)
    {
        // cerr<<count<<'\n';
        Row ind_row(it->GetRowId());
        it->GetKeyFromRow(table_info->GetSchema(),index_info->GetIndexKeySchema(),ind_row);
        // cerr<<"To insert "<<ind_row.GetRowId().GetPageId()<<' '<<ind_row.GetRowId().GetSlotNum()<<"\n";
        // cerr<<"To insert "<<it->GetRowId().GetPageId()<<' '<<it->GetRowId().GetSlotNum()<<"\n";
        index_info->GetIndex()->InsertEntry(ind_row,it->GetRowId(),nullptr);
        count++;
    }
// cerr<<"mimi\n";
    (catalog_meta_->index_meta_pages_)[next_index_id_]=meta_page_id;
    auto it_index=index_names_.find(table_name);
    if(it_index==index_names_.end())
    {
        std::unordered_map<std::string, index_id_t> temp;
        temp.emplace(std::unordered_map<std::string, index_id_t>::value_type(index_name,next_index_id_));
        index_names_.emplace(std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>::value_type(table_name,temp));
    }
    else it_index->second.emplace(std::unordered_map<std::string, index_id_t>::value_type(index_name,next_index_id_));
    indexes_.insert(std::unordered_map<index_id_t, IndexInfo *>::value_type(next_index_id_,index_info));
    next_index_id_++;//update catalog metadata
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const 
{
    auto index=index_names_.find(table_name);
    if(index==index_names_.end()) return DB_INDEX_NOT_FOUND;
    auto it=index->second.find(index_name);     
    if(it==index->second.end()) return DB_INDEX_NOT_FOUND;  
    index_id_t index_id=it->second;
    auto info=indexes_.find(index_id);
    if(info==indexes_.end()) return DB_INDEX_NOT_FOUND;
    index_info=info->second;
    return DB_SUCCESS;               
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    auto it=index_names_.find(table_name);
    if(it==index_names_.end()) return DB_INDEX_NOT_FOUND;
    for(auto index:it->second)
    {
        index_id_t index_id=index.second;
        auto info=indexes_.find(index_id);
        if(info==indexes_.end()) return DB_INDEX_NOT_FOUND;
        indexes.push_back(info->second);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
    TableInfo* table_info=TableInfo::Create();
    if(GetTable(table_name,table_info)==DB_TABLE_NOT_EXIST) return DB_TABLE_NOT_EXIST; 

    table_id_t table_id=table_names_[table_name];
    table_names_.erase(table_name);
    tables_.erase(table_id);
    catalog_meta_->table_meta_pages_.erase(table_id);//update catalog metadata
    if(!buffer_pool_manager_->DeletePage(table_id)) return DB_FAILED;//delete meta page

    TableHeap* now_heap =table_info->GetTableHeap();
    now_heap->DeleteTable(now_heap->GetFirstPageId());//delete heap

    delete table_info;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
    IndexInfo* index_info=IndexInfo::Create();
    if(GetIndex(table_name,index_name,index_info)==DB_INDEX_NOT_FOUND) return DB_INDEX_NOT_FOUND;

    index_id_t index_id=index_info->meta_data_->GetIndexId();
    page_id_t page_id=catalog_meta_->index_meta_pages_[index_id];
    if(!buffer_pool_manager_->DeletePage(page_id)) return DB_FAILED;//delete meta page

    auto it=index_names_.find(table_name);
    it->second.erase(index_name);
    indexes_.erase(index_id);
    catalog_meta_->index_meta_pages_.erase(index_id);//update catalog metadata

    delete index_info;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
    TablePage* cata_meta_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID));
    cata_meta_page->WLatch();
    catalog_meta_->SerializeTo(cata_meta_page->GetData());//write back catalog metadata
    cata_meta_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
    auto it=catalog_meta_->table_meta_pages_.find(table_id);
    if(it==catalog_meta_->table_meta_pages_.end()) return DB_TABLE_ALREADY_EXIST;//has been in it
    catalog_meta_->table_meta_pages_.insert(std::map<table_id_t, page_id_t>::value_type(table_id,page_id));//load in
    next_table_id_=catalog_meta_->GetNextTableId();//update next id

    TablePage* meta_page=reinterpret_cast<TablePage*> (buffer_pool_manager_->FetchPage(page_id));
    TableMetadata* meta_data=TableMetadata::Create(table_id,"",INVALID_PAGE_ID,nullptr);
    meta_page->RLatch();
    meta_data->DeserializeFrom(meta_page->GetData(),meta_data);
    meta_page->RUnlatch();
    TableInfo* table_info=TableInfo::Create();
    TableHeap* table_heap=TableHeap::Create(buffer_pool_manager_,meta_data->GetFirstPageId(),meta_data->GetSchema(),log_manager_,lock_manager_);
    table_info->Init(meta_data,table_heap);

    buffer_pool_manager_->UnpinPage(page_id,false);

    table_names_.insert(std::unordered_map<std::string, table_id_t>::value_type(meta_data->GetTableName(),table_id));
    tables_.insert(std::unordered_map<table_id_t, TableInfo *>::value_type(table_id,table_info));//load in
    return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    auto it=catalog_meta_->index_meta_pages_.find(index_id);
    if(it==catalog_meta_->index_meta_pages_.end()) return DB_INDEX_ALREADY_EXIST;
    catalog_meta_->index_meta_pages_.insert(std::map<index_id_t, page_id_t>::value_type(index_id,page_id));//load in
    next_index_id_=catalog_meta_->GetNextIndexId();//update next id

    TablePage* meta_page=reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
    std::vector<uint32_t> temp;
    IndexMetadata* meta_data=IndexMetadata::Create(index_id,"",-1,temp);
    meta_page->RLatch();
    meta_data->DeserializeFrom(meta_page->GetData(),meta_data);
    meta_page->RUnlatch();

    IndexInfo* index_info=IndexInfo::Create();
    table_id_t table_id=meta_data->GetTableId();
    auto info=tables_.find(table_id);//get the corresponding tableinfo
    if(info==tables_.end()) return DB_FAILED;
    index_info->Init(meta_data,info->second,buffer_pool_manager_);

    buffer_pool_manager_->UnpinPage(page_id,false);

    index_names_[info->second->GetTableName()].insert(std::unordered_map<std::string, index_id_t>::value_type(meta_data->GetIndexName(),index_id));
    indexes_.insert(std::unordered_map<index_id_t, IndexInfo *>::value_type(index_id,index_info));//load in
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto it=tables_.find(table_id);
    if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
    table_info=it->second;
    return DB_SUCCESS;
}
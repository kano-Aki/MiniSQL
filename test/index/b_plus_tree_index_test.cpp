#include "index/b_plus_tree_index.h"

#include <string>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/generic_key.h"

static const std::string db_name = "bp_tree_index_test.db";

TEST(BPlusTreeTests, BPlusTreeIndexGenericKeyTest) {
  // cerr<<"anarios\n";
  DBStorageEngine engine(db_name);
  // cerr<<"capadocia\n";
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<uint32_t> index_key_map{0, 1};
  const TableSchema table_schema(columns);
  // cerr<<"cilicia\n";
  auto *key_schema = Schema::ShallowCopySchema(&table_schema, index_key_map);
  // cerr<<"trebizond\n";
  std::vector<Field> fields{Field(TypeId::kTypeInt, 27),
                            Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
  KeyManager KP(key_schema, 128);
  Row key(fields);
  GenericKey *k1 = KP.InitKey();
  KP.SerializeFromKey(k1, key, key_schema);
  GenericKey *k2 = KP.InitKey();
  Row copy_key(fields);
  // cerr<<"Sinope\n";
  KP.SerializeFromKey(k2, copy_key, key_schema);
  ASSERT_EQ(0, KP.CompareKeys(k1, k2));
}

TEST(BPlusTreeTests, BPlusTreeIndexSimpleTest) {
  //  using INDEX_KEY_TYPE = GenericKey<32>;
  //  using INDEX_COMPARATOR_TYPE = GenericComparator<32>;
  //  using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;
  DBStorageEngine engine(db_name);
  // cerr<<"Athens\n";
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<uint32_t> index_key_map{0, 1};
  // cerr<<"Thebes\n";
  const TableSchema table_schema(columns);
  // cerr<<"Sparta\n";
  auto *index_schema = Schema::ShallowCopySchema(&table_schema, index_key_map);
  // cerr<<"Kolins\n";
  auto *index = new BPlusTreeIndex(0, index_schema, 256, engine.bpm_);
  // cerr<<"Edirne\n";
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->InsertEntry(row, rid, nullptr));
  }
  // cerr<<"Abydos\n";
  // Test Scan
  std::vector<RowId> ret;
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->ScanKey(row, ret, nullptr));
    ASSERT_EQ(rid.Get(), ret[i].Get());
    // cerr<<i<<' '<<ret[i].GetPageId()<<' '<<ret[i].GetSlotNum()<<'\n';
  }
  // Test Delete
  ret.clear();
  // cerr<<"ret.size()="<<ret.size()<<'\n';
  for (int i = 0; i < 10; i++) {
    // cerr<<"i="<<i<<'\n';
    std::vector<Field> fields{Field(TypeId::kTypeInt, i),
                              Field(TypeId::kTypeChar, const_cast<char *>("minisql"), 7, true)};
    Row row(fields);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->RemoveEntry(row, rid, nullptr));
    ASSERT_EQ(DB_KEY_NOT_FOUND, index->ScanKey(row, ret, nullptr));
    ASSERT_TRUE(ret.size()==0);
  }
  // cerr<<"Pela\n";
  // Iterator Scan
  IndexIterator iter = index->GetBeginIterator();
  // cerr<<"Salonica\n";
  uint32_t i = 0;
  for (; iter != index->GetEndIterator(); ++iter) {
    ASSERT_EQ(1000, (*iter).second.GetPageId());
    ASSERT_EQ(i, (*iter).second.GetSlotNum());
    i++;
  }
  delete index;
}
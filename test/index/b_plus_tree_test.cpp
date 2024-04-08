#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  std::vector<Column *> columns = {
      new Column("int", TypeId::kTypeInt, 0, false, false),
  };
  Schema *table_schema = new Schema(columns);
  KeyManager KP(table_schema, 256);
  BPlusTree tree(0, engine.bpm_, KP);
  TreeFileManagers mgr("tree_");
  cerr<<"koko?\n";
  // Prepare data
  const int n = 20000;
  vector<GenericKey *> keys;
  vector<RowId> values;
  vector<GenericKey *> delete_seq;
  map<GenericKey *, RowId> kv_map;
  cerr<<"nani?\n";
  for (int i = 0; i < n; i++) {
    // cerr<<"i="<<i<<'\n';
    GenericKey *key = KP.InitKey();
    // cerr<<"azhe\n";
    std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
    // cerr<<"ana\n";
    KP.SerializeFromKey(key, Row(fields), table_schema);
    // cerr<<"aly\n";
    keys.push_back(key);
    // cerr<<"aba\n";
    values.push_back(RowId(i));
    // cerr<<"ava\n";
    delete_seq.push_back(key);
    // cerr<<"azn\n";
  }
  vector<GenericKey *> keys_copy(keys);
  cerr<<"soko?\n";
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  cerr<<"asoko?\n";
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  cerr<<"kore\n";
  // Insert data
  for (int i = 0; i < n; i++) {
    // cerr<<"i="<<i<<'\n';
    tree.Insert(keys[i], values[i]);
    // ASSERT_TRUE(tree.Check());
  }
  ASSERT_TRUE(tree.Check());
  cerr<<"sore\n";
  // Print tree
  // tree.PrintTree(mgr[0]);
  cerr<<"are\n";
  // Search keys
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    // cerr<<"Search Keys: i="<<i<<'\n';
    tree.GetValue(keys_copy[i], ans);
    // cerr<<"Search Key finish\n";
    ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
  }
  // ASSERT_TRUE(tree.Check());
  cerr<<"kono\n";
  // tree.PrintTreee();
  // ASSERT_TRUE(tree.Check());
  // return;
  // Delete half keys
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    // cerr<<"Deleting i="<<i<<'\n';
    tree.Remove(delete_seq[i]);
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
    // ASSERT_TRUE(tree.Check());
    // tree.PrintTreee();
  }
  // ASSERT_TRUE(tree.Check());
  // tree.PrintTree(mgr[1]);
  cerr<<"sono-----------------------------\n";
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  // ASSERT_TRUE(tree.Check());
  cerr<<"ano\n";
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
  ASSERT_TRUE(tree.Check());
}
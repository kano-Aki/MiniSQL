#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"
#include "page/table_page.h"
class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator()=default;

  explicit TableIterator(page_id_t page_id,Row& row,BufferPoolManager* manager,TableHeap* table_heap):now_page_id(page_id),now_row(row),buffer_pool_manager_(manager),table_heap_(table_heap) {}

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator()=default;

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

  page_id_t getNowPageId() const {return now_page_id;}
  Row getNowRow() const {return now_row;}
  BufferPoolManager* getManager() const {return buffer_pool_manager_;}
  TableHeap* getTableHeap() const {return table_heap_;}
  void setNowRid(Row& row){now_row=row;}
  void setManager(BufferPoolManager* manager) {buffer_pool_manager_=manager;}
  void setTableHeap(TableHeap* table_heap) {table_heap_=table_heap;}

private:
    page_id_t now_page_id;
    Row now_row;
    BufferPoolManager *buffer_pool_manager_;
    TableHeap* table_heap_;
};

#endif  // MINISQL_TABLE_ITERATOR_H

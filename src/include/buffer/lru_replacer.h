#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <map>
#include <vector>
#include <unordered_map>
#include "buffer/replacer.h"
#include "common/config.h"

using num_access = int;
using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer();

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here
  list<frame_id_t> lru_list;//record the frame id of unpined page
  //the begin of lru_list is the next to be replaced page
  //when one page is accessed,move it to the end
};

#endif  // MINISQL_LRU_REPLACER_H

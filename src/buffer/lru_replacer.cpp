#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer() = default;
LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if(lru_list.empty()) return false;//no page to victim
    *frame_id=*(lru_list.begin());
    lru_list.remove(*frame_id);
    return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    lru_list.remove(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
    for(auto it:lru_list)
    {
        if(it==frame_id) return;//has been unpin
    }
    lru_list.push_back(frame_id);//insert to the end
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}
#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  uint32_t byte_index=next_free_page_/8,bit_index=next_free_page_%8;
  if(page_allocated_<8*MAX_CHARS)
  {
    bytes[byte_index]+=1<<(7-bit_index);////add the corresponding bits
    page_offset=next_free_page_;
    page_allocated_++;
    for(uint32_t i=next_free_page_;i<8*MAX_CHARS;i++)
    {
        if(IsPageFree(i)) 
        {
            next_free_page_=i;//update infomation
            break;
        }
    }
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint32_t byte_index=page_offset/8,bit_index=page_offset%8;
  if(page_offset<8*MAX_CHARS) 
  {
    if(IsPageFree(page_offset)) return false;//is free before
    bytes[byte_index]-=1<<(7-bit_index);////Subtract the corresponding bits
    page_allocated_--;
    if(page_offset<next_free_page_) next_free_page_=page_offset;
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index=page_offset/8,bit_index=page_offset%8;
  if((bytes[byte_index]>>(7-bit_index))%2==0) return true;////Move the bit to be judged right to the lowest bit, and then judge parity
  return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
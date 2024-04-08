#include"../include/buffer/clock_replacer.h"

CLOCKReplacer:: CLOCKReplacer(size_t num_pages)
{
    capacity=num_pages;
    now_loc=0;
    for(size_t i=0;i<num_pages;i++) clock_status.insert(std::map<frame_id_t, frame_id_t>::value_type(i,0));
}

CLOCKReplacer::  ~CLOCKReplacer()=default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id)
{
    frame_id_t now_id;
    if(now_loc==clock_list.size()) now_loc=0;//init
    auto it=clock_list.begin();
    for(int i=0;i<now_loc;i++) it++;
    while(1)
    {
        now_id=*it;
        if(clock_status[now_id]==0)
        {
            *frame_id=now_id;
            clock_list.remove(now_id);
            return true;
        }
        else{
            clock_status[now_id]=0;
            now_loc=(now_loc+1)%clock_list.size();
            it++;
            if(it==clock_list.end()) it=clock_list.begin();
        }
    }
}

void CLOCKReplacer::Pin(frame_id_t frame_id)
{
    clock_list.remove(frame_id);
    clock_status[frame_id]=1;
}

void CLOCKReplacer::Unpin(frame_id_t frame_id)
{
    for(auto it=clock_list.begin();it!=clock_list.end();it++)
    {
        if(*it==frame_id) return;
    }
    clock_list.push_back(frame_id);
}

size_t CLOCKReplacer::Size()
{
    return clock_list.size();
}
#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
    uint32_t offset=0,num_column=columns_.size();
    memcpy(buf+offset,&SCHEMA_MAGIC_NUM,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    memcpy(buf+offset,&num_column,sizeof(uint32_t));
    offset+=sizeof(uint32_t);//number of column
    std::vector<Column *>::const_iterator it;
    for(it=columns_.begin();it!=columns_.end();it++)
    {
        offset+=(*it)->SerializeTo(buf+offset);//just store the Column instead of pointer to avoid double delete
    }
    memcpy(buf+offset,&is_manage_,sizeof(bool));
    offset+=sizeof(bool);
    return offset;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t offset=sizeof(uint32_t)*2+sizeof(bool);
    for(auto it:columns_)
    {
        offset+=it->GetSerializedSize();
    }
    return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    uint32_t magic,offset=0,num_column;
    memcpy(&magic,buf+offset,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    if(magic!=SCHEMA_MAGIC_NUM) return 0;
    memcpy(&num_column,buf+offset,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    schema->columns_.clear();
    for(uint32_t i=0;i<num_column;i++)
    {
        Column* temp=new Column("",TypeId::kTypeInt,-1,true,true);
        offset+=Column::DeserializeFrom(buf+offset,temp);
        schema->columns_.push_back(temp);
    }
    memcpy(&schema->is_manage_,buf+offset,sizeof(bool));
    offset+=sizeof(bool);
    return offset;
}
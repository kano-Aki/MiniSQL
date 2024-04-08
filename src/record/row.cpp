#include "record/row.h"
class NULLbitmap{
public:    
    NULLbitmap()
    {
        number=0;
        all_null=true;
    }
    NULLbitmap(const std::vector<Field *>& fields_)
    {
        number=fields_.size();
        all_null=true;
        int off=0;
        std::vector<Field *>::const_iterator it;
        for(it=fields_.begin();it!=fields_.end();it++)
        {
            if((*it)->IsNull()) isnull[off++]=true;
            else 
            {
                isnull[off++]=false;
                all_null=false;
            }
        }
    }
    ~NULLbitmap()=default;

    uint32_t number;
    bool all_null;
    bool isnull[0];//true means NULL,Flexible Array.not occupy space in class
};
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    if(fields_.size()==0) return 0;//empty
    uint32_t offset=0;

    NULLbitmap header(fields_);
    memcpy(buf+offset,&header.number,sizeof(uint32_t));
    offset+=sizeof(uint32_t);//not include isnull
    memcpy(buf+offset,&header.all_null,sizeof(bool));
    offset+=sizeof(bool);

    for(uint32_t i=0;i<header.number;i++)//isnull
    {
        memcpy(buf+offset,&header.isnull[i],sizeof(bool));
        offset+=sizeof(bool);
    }

    std::vector<Field *>::const_iterator it;
    TypeId type_id;
    for(it=fields_.begin();it!=fields_.end();it++)
    {
        if((*it)->IsNull()) continue;//skip it
        type_id=(*it)->GetTypeId();
        memcpy(buf+offset,&type_id,sizeof(TypeId));//store the type id
        offset+=sizeof(TypeId);
        offset+=(*it)->SerializeTo(buf+offset); //just store the Field instead of pointer to avoid double delete
        //call the SerializeTo of the field
    }
    return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");
    NULLbitmap header;
    uint32_t offset=0;

    memcpy(&header.number,buf+offset,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    memcpy(&header.all_null,buf+offset,sizeof(bool));
    offset+=sizeof(bool);
    if(header.all_null) return offset;//all null

    bool isnull[header.number];
    memcpy(isnull,buf+offset,sizeof(bool)*header.number);//read isnull
    offset+=sizeof(bool)*header.number;

    TypeId type_id;
    for(uint32_t i=0;i<header.number;i++)
    {
        if(isnull[i]) 
        {
            Field* nu=new Field(TypeId::kTypeInvalid);
            fields_.push_back(nu);
            continue;
        }
        Field** temp=new Field*;
        memcpy(&type_id,buf+offset,sizeof(TypeId));//get the type_id
        offset+=sizeof(TypeId);
        offset+=Field::DeserializeFrom(buf+offset,type_id,temp,isnull[i]);//call the corresponding DeserializeFrom
        fields_.push_back(*temp);
    }
    return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    if(fields_.size()==0) return 0;//empty
    uint32_t offset=sizeof(uint32_t)+sizeof(bool);
    std::vector<Field *>::const_iterator it;
    for(it=fields_.begin();it!=fields_.end();it++)
    {
        if(!(*it)->IsNull())
        {
            offset+=(*it)->GetSerializedSize();//call the field
            offset+=sizeof(TypeId);
        }
        
    }
    return offset+sizeof(bool)*fields_.size();//all fields are null,just return the sizeof header
    //since isnull does not occupy the space of NULLbitmap
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}

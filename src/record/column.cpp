#include "record/column.h"
#include<string>
#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
    uint32_t len_name=strlen(name_.c_str())+1,offset=0; 
    memcpy(buf+offset,&COLUMN_MAGIC_NUM,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    memcpy(buf+offset,&len_name,sizeof(uint32_t));//length of name
    offset+=sizeof(uint32_t);
    memcpy(buf+offset,name_.c_str(),len_name);//name
    offset+=len_name;//also read the '\0' for convenience
    memcpy(buf+offset,&type_,sizeof(TypeId));//type
    offset+=sizeof(TypeId);
    memcpy(buf+=offset,&len_,sizeof(uint32_t));//len_
    offset+=sizeof(uint32_t);
    memcpy(buf+offset,&table_ind_,sizeof(uint32_t));//ind
    offset+=sizeof(uint32_t);
    memcpy(buf+offset,&nullable_,sizeof(bool));//null
    offset+=sizeof(bool);
    memcpy(buf+offset,&unique_,sizeof(bool));//unique
    offset+=sizeof(bool);
    return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return strlen(name_.c_str())+1+sizeof(uint32_t)*4+sizeof(bool)*2+sizeof(TypeId);//a more length
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
    char* temp_name;
    uint32_t len_name,offset=0,magic;

    magic=MACH_READ_UINT32(buf);//read the first uint:magic
    if(magic!=COLUMN_MAGIC_NUM) return 0;//not match,error
    offset+=sizeof(uint32_t);
    memcpy(&len_name,buf+offset,sizeof(uint32_t));//length of name
    offset+=sizeof(uint32_t);
    temp_name=new char[len_name];
    memcpy(temp_name,buf+offset,len_name);//name
    offset+=len_name;
    column->name_=std::string(temp_name);
    memcpy(&column->type_,buf+offset,sizeof(TypeId));
    offset+=sizeof(TypeId);
    memcpy(&column->len_,buf+offset,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    memcpy(&column->table_ind_,buf+offset,sizeof(uint32_t));
    offset+=sizeof(uint32_t);
    memcpy(&column->nullable_,buf+offset,sizeof(bool));//null
    offset+=sizeof(bool);
    memcpy(&column->unique_,buf+offset,sizeof(bool));//unique
    offset+=sizeof(bool);
    delete temp_name;
    return offset;
}

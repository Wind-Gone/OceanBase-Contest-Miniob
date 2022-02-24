/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/13.
//

#include <limits.h>
#include <string.h>
#include <algorithm>
#include <queue>
#include <unordered_set>


#include "storage/common/table.h"
#include "common/io/io.h"
#include "storage/common/table_meta.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/default/disk_buffer_pool.h"
#include "storage/common/record_manager.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/common/index.h"
#include "storage/common/bplus_tree_index.h"
#include "storage/trx/trx.h"
#include "storage/common/date.h"
#include "sql/executor/tuple.h"

using namespace common;

Table::Table() : 
    data_buffer_pool_(nullptr),
    file_id_(-1),
    record_handler_(nullptr) {
}

Table::~Table() {
  delete record_handler_;
  record_handler_ = nullptr;

  if (data_buffer_pool_ != nullptr && file_id_ >= 0) {
    data_buffer_pool_->close_file(file_id_);
    data_buffer_pool_ = nullptr;
  }

  LOG_INFO("Table has been closed: %s", name());
}

RC Table::create(const char *path, const char *name, const char *base_dir, int attribute_count, const AttrInfo attributes[]) {

  if (nullptr == name || common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attribute_count <= 0 || nullptr == attributes) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d, attributes=%p",
        name, attribute_count, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在

  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (-1 == fd) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s",
                path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", 
       path, errno, strerror(errno));
    return RC::IOERR;
  }

  close(fd);

  // 创建文件
  if ((rc = table_meta_.init(name, attribute_count, attributes)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc; // delete table file
  }

  std::fstream fs;
  fs.open(path, std::ios_base::out | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR;
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs);
  fs.close();

  std::string data_file = std::string(base_dir) + "/" + name + TABLE_DATA_SUFFIX;
  data_buffer_pool_ = theGlobalDiskBufferPool();
  rc = data_buffer_pool_->create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  rc = init_record_handler(base_dir);

  base_dir_ = base_dir;
  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  return rc;
}
RC Table::drop(){
  const char *name = this->name();
  if (nullptr == name || common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to drop table %s:%s", base_dir_.c_str(), name);

  RC rc = RC::SUCCESS;

  for (Index *index : this->indexes_)
  {
    std::string index_file = index_data_file(base_dir_.c_str(), name, index->index_meta().name());
    if (remove(index_file.c_str()) != 0) {
      LOG_ERROR("Failed to drop bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
      return RC::IOERR_DELETE;
    }
    delete index;
    index = nullptr;
  }


  if(data_buffer_pool_->is_file_open(file_id_)){
    rc = data_buffer_pool_->close_file(file_id_);
  }

  if(rc != RC::SUCCESS){
    return rc;
  }

  std::string table_file_path = table_meta_file(base_dir_.c_str(), name);
  std::string data_file = base_dir_ + "/" + name + TABLE_DATA_SUFFIX;
  if(remove(data_file.c_str()) != 0 ||remove(table_file_path.c_str()) != 0 ){
    rc = RC::IOERR_DELETE;
  }
  if(rc != RC::SUCCESS){
    return rc;
  }


  for (size_t i = 0; i < table_meta_.test_num(); i++){
    std::string file_name = base_dir_ + "/" + std::string(table_meta_.name()) + "-test-" + std::to_string(i);
    if( access(file_name.c_str(), F_OK) != -1){
      if (remove(file_name.c_str()) != 0) {
        rc = RC::IOERR_DELETE;
        break;
      }
    }
  }
  
  if(rc != RC::SUCCESS){
    return rc;
  }

  LOG_INFO("Successfully drop table %s:%s", base_dir_.c_str(), name);
  return rc;
}
RC Table::open(const char *meta_file, const char *base_dir) {
  // 加载元数据文件
  std::fstream fs;
  std::string meta_file_path = std::string(base_dir) + "/" + meta_file;
  fs.open(meta_file_path, std::ios_base::in | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file, strerror(errno));
    return RC::IOERR;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file);
    return RC::GENERIC_ERROR;
  }
  fs.close();

  // 加载数据文件
  RC rc = init_record_handler(base_dir);

  base_dir_ = base_dir;

  const int index_num = table_meta_.index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i);
    std::vector<std::string> fields = index_meta->fields();
    const FieldMeta *field_metas[fields.size()];
    for(size_t i = 0; i < fields.size(); ++i){
      std::string _field = fields[i];
      const FieldMeta *field_meta = table_meta_.field(_field.c_str());
      
      if (field_meta == nullptr) {
        LOG_PANIC("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                  name(), index_meta->name(), _field.c_str());
        return RC::GENERIC_ERROR;
      }
      field_metas[i] = field_meta;
    }
    

    BplusTreeIndex *index = new BplusTreeIndex();
    
    std::string index_file = index_data_file(base_dir, name(), index_meta->name());
    rc = index->open(index_file.c_str(), *index_meta, *field_metas[0]);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%d:%s",
                name(), index_meta->name(), index_file.c_str(), rc, strrc(rc));
      return rc;
    }
    indexes_.push_back(index);
  }
  return rc;
}

RC Table::commit_insert(Trx *trx, const RID &rid) {
  Record record;
  RC rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return trx->commit_insert(this, record);
}

RC Table::rollback_insert(Trx *trx, const RID &rid) {

  Record record;
  RC rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // remove all indexes
  rc = delete_entry_of_indexes(record.data, rid, false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete indexes of record(rid=%d.%d) while rollback insert, rc=%d:%s",
              rid.page_num, rid.slot_num, rc, strrc(rc));
  } else {
    rc = record_handler_->delete_record(&rid);
  }
  return rc;
}

RC Table::insert_record(Trx *trx, Record *record) {
  RC rc = RC::SUCCESS;

  if (trx != nullptr) {
    trx->init_trx_info(this, *record);
  }
  rc = record_handler_->insert_record(record->data, table_meta_.record_size(), &record->rid);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%d:%s", table_meta_.name(), rc, strrc(rc));
    return rc;
  }

  if (trx != nullptr) {
    rc = trx->insert_record(this, record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to log operation(insertion) to trx");

      RC rc2 = record_handler_->delete_record(&record->rid);
      if (rc2 != RC::SUCCESS) {
        LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                  name(), rc2, strrc(rc2));
      }
      return rc;
    }
  }

  rc = insert_entry_of_indexes(record->data, record->rid);
  if (rc != RC::SUCCESS) {
    RC rc2 = delete_entry_of_indexes(record->data, record->rid, true);
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record->rid);
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    return rc;
  }
  return rc;
}
RC Table::insert_record(Trx *trx, int tuple_num, const InsertTuple *tuples) {
  if (tuple_num <= 0 || nullptr == tuples ) {
    LOG_ERROR("Invalid argument. value num=%d, values=%p", tuple_num, tuples);
    return RC::INVALID_ARGUMENT;
  }
  //std::queue<Record> insert_records;
  RC rc = RC::SUCCESS;
  for (int i = 0; i < tuple_num; ++i){
    char *record_data;
    int value_num = tuples[i].value_num;
    const Value* values = tuples[i].values;
    rc = make_record(value_num, values, record_data);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to create a record. rc=%d:%s", rc, strrc(rc));
      if (nullptr != trx)
      {
        trx->rollback();
      }
      
      return rc;
    }
    Record record;
    record.data = record_data;
    // record.valid = true;
    rc = insert_record(trx, &record);
    if (rc != RC::SUCCESS) {
      if (nullptr != trx)
      {
        trx->rollback();
      }
      return rc;      
    }
    //insert_records.push(record);
  }
  return rc;
}

RC Table::insert_record(Trx *trx, int value_num, const Value *values) {
  if (value_num <= 0 || nullptr == values ) {
    LOG_ERROR("Invalid argument. value num=%d, values=%p", value_num, values);
    return RC::INVALID_ARGUMENT;
  }

  char *record_data;
  RC rc = make_record(value_num, values, record_data);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create a record. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  Record record;
  record.data = record_data;
  // record.valid = true;
  rc = insert_record(trx, &record);
  delete[] record_data;
  return rc;
}


const char *Table::name() const {
  return table_meta_.name();
}

const TableMeta &Table::table_meta() const {
  return table_meta_;
}

void Table::copy_data(const Value& copy_from, const FieldMeta * field_meta, char* copy_to) {
  if(field_meta->type() == TEXTS){
    std::string file_name = base_dir_ + "/" + std::string(table_meta_.name()) + "-test-" + std::to_string(table_meta_.test_num());
    writeToFile(file_name, (char *)copy_from.data, std::min(4096,(int) strlen((char*) copy_from.data) + 1),"w");
    int test_num = table_meta_.test_num();
    memcpy(copy_to, &test_num, field_meta->len());
    table_meta_.increase_test_num();
  }
  else{
    if (copy_from.type == DATES || copy_from.type == CHARS) {
      memcpy(copy_to, copy_from.data, std::min(field_meta->len(), (int) strlen((char*) copy_from.data) + 1)); 
    } else {
      memcpy(copy_to, copy_from.data, field_meta->len());
    }
  }


  bool is_null = copy_from.type == NONE;
  memcpy(copy_to + field_meta->len(), &is_null, sizeof(bool));
}
RC Table::make_record(int value_num, const Value *values, char * &record_out) {
  // 检查字段类型是否一致
  if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int i = 0; i < value_num; i++)
  {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value &value = values[i];
    if (field_type_compare_compatible_table(field->type(), value.type) != RC::SUCCESS)
    {
      LOG_ERROR("Invalid value type. field name=%s, type=%d, but given=%d",
                field->name(), field->type(), value.type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    if (field->type() == DATES && value.type == CHARS)
    {
      char *new_val = reinterpret_cast<char *>(values[i].data);
      Date date;
      RC rc = date.readAndValidate(new_val);
      if (rc == RC::SCHEMA_FIELD_TYPE_MISMATCH) {
        LOG_ERROR("Invalid value type. field name=%s, type=%d, but given=%d", field->name(), field->type(), value.type);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      date.writeToChars(new_val);
    }
  }

  // 复制所有字段的值
  int record_size = table_meta_.record_size();
  char *record = new char [record_size];

  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value &value = values[i];
    // 如果是unique属性，先检查有没有重复的
    if (field->unique())  {
      RC rc = check_unique(field, &value);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
    if (value.type == NONE && !field->nullable()){
      return RC::CONSTRAINT_NOTNULL;
    }
    
    copy_data(value, field, record + field->offset());
  }

  record_out = record;
  return RC::SUCCESS;
}

RC Table::init_record_handler(const char *base_dir) {
  std::string data_file = std::string(base_dir) + "/" + table_meta_.name() + TABLE_DATA_SUFFIX;
  if (nullptr == data_buffer_pool_) {
    data_buffer_pool_ = theGlobalDiskBufferPool();
  }

  int data_buffer_pool_file_id;
  RC rc = data_buffer_pool_->open_file(data_file.c_str(), &data_buffer_pool_file_id);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s",
              data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler();
  rc = record_handler_->init(*data_buffer_pool_, data_buffer_pool_file_id);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  file_id_ = data_buffer_pool_file_id;
  return rc;
}

/**
 * 为了不把Record暴露出去，封装一下
 */
class RecordReaderScanAdapter {
public:
  explicit RecordReaderScanAdapter(void (*record_reader)(const char *data, void *context), void *context)
      : record_reader_(record_reader), context_(context){
  }

  void consume(const Record *record) {
    record_reader_(record->data, context_);
  }
private:
  void (*record_reader_)(const char *, void *);
  void *context_;
};
static RC scan_record_reader_adapter(Record *record, void *context) {
  RecordReaderScanAdapter &adapter = *(RecordReaderScanAdapter *)context;
  adapter.consume(record);
  return RC::SUCCESS;
}

RC Table::scan_record(Trx *trx, ConditionFilter *filter, int limit, void *context, void (*record_reader)(const char *data, void *context)) {
  RecordReaderScanAdapter adapter(record_reader, context);
  return scan_record(trx, filter, limit, (void *)&adapter, scan_record_reader_adapter);
}

RC Table::scan_record(Trx *trx, ConditionFilter *filter, int limit, void *context, RC (*record_reader)(Record *record, void *context)) {
  if (nullptr == record_reader) {
    return RC::INVALID_ARGUMENT;
  }

  if (0 == limit) {
    return RC::SUCCESS;
  }

  if (limit < 0) {
    limit = INT_MAX;
  }

  IndexScanner *index_scanner = find_index_for_scan(filter);
  if (index_scanner != nullptr) {
    return scan_record_by_index(trx, index_scanner, filter, limit, context, record_reader);
  }

  RC rc = RC::SUCCESS;
  RecordFileScanner scanner;
  rc = scanner.open_scan(*data_buffer_pool_, file_id_, filter);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. file id=%d. rc=%d:%s", file_id_, rc, strrc(rc));
    return rc;
  }

  int record_count = 0;
  Record record;
  rc = scanner.get_first_record(&record);
  for ( ; RC::SUCCESS == rc && record_count < limit; rc = scanner.get_next_record(&record)) {
    if (trx == nullptr || trx->is_visible(this, &record)) {
      rc = record_reader(&record, context);
      if (rc != RC::SUCCESS) {
        break;
      }
      record_count++;
    }
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  } else {
    LOG_ERROR("failed to scan record. file id=%d, rc=%d:%s", file_id_, rc, strrc(rc));
  }
  scanner.close_scan();
  return rc;
}

RC Table::scan_record_by_index(Trx *trx, IndexScanner *scanner, ConditionFilter *filter, int limit, void *context,
                               RC (*record_reader)(Record *, void *)) {
  RC rc = RC::SUCCESS;
  RID rid;
  Record record;
  int record_count = 0;
  while (record_count < limit) {
    rc = scanner->next_entry(&rid);
    if (rc != RC::SUCCESS) {
      if (RC::RECORD_EOF == rc) {
        rc = RC::SUCCESS;
        break;
      }
      LOG_ERROR("Failed to scan table by index. rc=%d:%s", rc, strrc(rc));
      break;
    }

    rc = record_handler_->get_record(&rid, &record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to fetch record of rid=%d:%d, rc=%d:%s", rid.page_num, rid.slot_num, rc, strrc(rc));
      break;
    }

    if ((trx == nullptr || trx->is_visible(this, &record)) && (filter == nullptr || filter->filter(record))) {
      rc = record_reader(&record, context);
      if (rc != RC::SUCCESS) {
        LOG_TRACE("Record reader break the table scanning. rc=%d:%s", rc, strrc(rc));
        break;
      }
    }

    record_count++;
  }

  scanner->destroy();
  return rc;
}

class IndexInserter {
public:
  explicit IndexInserter(Index *index) : index_(index) {
  }

  RC insert_index(const Record *record) {
    return index_->insert_entry(record->data, &record->rid);
  }
private:
  Index * index_;
};

static RC insert_index_record_reader_adapter(Record *record, void *context) {
  IndexInserter &inserter = *(IndexInserter *)context;
  return inserter.insert_index(record);
}


bool is_equal(const Value &v1, const Value &v2){
  switch (v1.type) {
    case INTS: {
      return *(int*)(v1.data) == *(int*)(v2.data);
    }
    break;
    case FLOATS: {
      return *(float *)(v1.data) == *(float *)(v2.data);
    }
      break;
    case DATES:
    case CHARS: {
      return strcmp((char *)v1.data,(char *)v2.data) == 0;
    }
    break;
    
    default: {
      LOG_PANIC("Unsupported field type. type=%d",v1.type);
      return false;
    }
  }
}


// 检查这个属性列有没有重复值，用hash
RC Table::check_repeat(const FieldMeta *field_meta){
  // TODO
  RC rc = RC::SUCCESS;
  
  
  RecordFileScanner scanner;

  rc = scanner.open_scan(*data_buffer_pool_, file_id_, nullptr);
  if (rc != RC::SUCCESS){
    return rc;
  }
  Record record;

  rc = scanner.get_first_record(&record);

  // set的hash函数
  class hash_value{
    public:
    // 把指针视为一个int进行hash
    size_t operator()(const Value& value)const{
      return std::hash<int>()(*(int*)value.data);
    }
  };
  // value的比较函数，假定两者类型是相同的
  class equal_value{
    public:
    bool operator()(const Value &v1, const Value &v2)const{
      return is_equal(v1,v2);
    }
  };


  std::unordered_set<Value,hash_value,equal_value> record_set;
  for ( ; RC::SUCCESS == rc; rc = scanner.get_next_record(&record)) {
    Value value;
    value.type = field_meta->type();
    value.data = record.data + field_meta->offset();

    if (record_set.count(value))
    {
      scanner.close_scan();
      return RC::CONSTRAINT_UNIQUE;
    }
    record_set.insert(value);
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  } else {
    LOG_ERROR("failed to scan record. file id=%d, rc=%d:%s", file_id_, rc, strrc(rc));
  }

  rc = scanner.close_scan();

  
  
  return rc;
}

RC Table::create_index(Trx *trx, const char *index_name, char * const*attribute_names, const int attribute_num, const bool unique) {
  RC rc = RC::SUCCESS;
  const FieldMeta *field_metas[attribute_num] ;
  if (table_meta_.index(index_name) != nullptr ||
    table_meta_.find_index_by_fields(attribute_names, attribute_num)) {
    return RC::SCHEMA_INDEX_EXIST;
  }

  for (size_t i = 0;i < attribute_num ; ++i){
    const char * attribute_name = attribute_names[i];
    if (index_name == nullptr || common::is_blank(index_name) ||
    attribute_name == nullptr || common::is_blank(attribute_name)) {
      return RC::INVALID_ARGUMENT;
    }

    const FieldMeta *field_meta = table_meta_.field(attribute_name);
    if (!field_meta) {
      return RC::SCHEMA_FIELD_MISSING;
    }
    field_metas[i] = field_meta;// 倒序
    // 如果是唯一索引，先判断当前存不存在重复，唯一索引和多列索引不会同时出现，所以放这里没有关系
    if (unique){
      if(this->check_repeat(field_meta) == RC::SUCCESS){
        rc =  table_meta_.set_field_unique(field_meta->name(),true);
        if (rc != RC::SUCCESS){
          return rc;
        }
      }
      else{// 不存在，就更新信息
        return RC::CONSTRAINT_UNIQUE;
      }
    }
  }

  IndexMeta new_index_meta;
  rc = new_index_meta.init(index_name, field_metas, attribute_num);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 创建索引相关数据
  BplusTreeIndex *index = new BplusTreeIndex();
  std::string index_file = index_data_file(base_dir_.c_str(), name(), index_name);
  rc = index->create(index_file.c_str(), new_index_meta, *field_metas[0]);
  // delete index file ?
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // 遍历当前的所有数据，插入这个索引
  IndexInserter index_inserter(index);
  rc = scan_record(trx, nullptr, -1, &index_inserter, insert_index_record_reader_adapter);
  if (rc != RC::SUCCESS) {
    // rollback
    delete index;
    LOG_ERROR("Failed to insert index to all records. table=%s, rc=%d:%s", name(), rc, strrc(rc));
    return rc;
  }
  index->sync();
  indexes_.push_back(index);

  TableMeta new_table_meta(table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
    return rc;
  }
  // 创建元数据临时文件
  std::string tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
  std::fstream fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR; // 创建索引中途出错，要做还原操作
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR;
  }
  fs.close();

  // 覆盖原始元数据文件
  std::string meta_file = table_meta_file(base_dir_.c_str(), name());
  int ret = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). " \
              "system error=%d:%s", tmp_file.c_str(), meta_file.c_str(), index_name, name(), errno, strerror(errno));
    return RC::IOERR;
  }

  table_meta_.swap(new_table_meta);

  LOG_INFO("add a new index (%s) on the table (%s)", index_name, name());

  return rc;
}

RC Table::drop_index(Trx *trx, const char *index_name){
  if (index_name == nullptr || common::is_blank(index_name)) {
    return RC::INVALID_ARGUMENT;
  }
  if (table_meta_.index(index_name) == nullptr) {
    return RC::SCHEMA_INDEX_NOT_EXIST;
  }
  Index *index = this->find_index(index_name);

  // 删除文件
  std::string index_file = index_data_file(base_dir_.c_str(), this->name(), index_name);
  if (remove(index_file.c_str()) != 0) {
    LOG_ERROR("Failed to delete index file. file name=%s", index_file.c_str());
    return RC::IOERR_DELETE;
  }

  // 从元信息中删除索引
  this->indexes_.erase( std::find(this->indexes_.begin(),this->indexes_.end(), index) );
  if (index->index_meta().fields().size() == 1) {// 只有单列才有唯一索引
    table_meta_.set_field_unique(index->index_meta().fields()[0].c_str(),false);
  }
  
  
  // 删除自身
  delete index;
  return RC::SUCCESS;
}

RC Table::update_record(Record * record,const FieldMeta * field_meta,const Value * value){
  copy_data(*value, field_meta, record->data + field_meta->offset());
  return this->record_handler_->update_record(record);
  //return RC::SUCCESS;
}

class RecordUpdater {
public:
  RecordUpdater(Trx *trx,const FieldMeta *field_meta, Table &table, const Value *value) 
  : trx_(trx), field_meta_(field_meta),table_(table), value_(value) {
  }

  RC update_record(Record *record) {
    RC rc = RC::SUCCESS;
    // 在这里修改record的内容，注意现在只用修改单个字段，所以扫描一下record的每个字段，更新对的字段就行了，注意跳过系统字段
    table_.update_record(record, field_meta_, value_);
    

        
    if (rc == RC::SUCCESS) {
      updated_count_++;
    }
    return rc;
  }

  int updated_count() const {
    return updated_count_; 
  }

private:
  Trx *trx_;
  const FieldMeta * field_meta_;
  Table & table_;
  const Value * value_;
  int updated_count_ = 0;
};



static RC record_reader_update_adapter(Record *record, void *context) {
  RecordUpdater &record_updater = *(RecordUpdater *)context;
  return record_updater.update_record(record);
}

RC Table::check_unique(const FieldMeta * field_meta, const Value * new_value){
  RC rc = RC::SUCCESS;
  
  
  RecordFileScanner scanner;

  rc = scanner.open_scan(*data_buffer_pool_, file_id_, nullptr);
  if (rc != RC::SUCCESS){
    return rc;
  }
  Record record;

  rc = scanner.get_first_record(&record);

  for ( ; RC::SUCCESS == rc; rc = scanner.get_next_record(&record)) {
    Value value;
    value.type = field_meta->type();
    value.data = record.data + field_meta->offset();

    if (is_equal(*new_value,value))
    {
      scanner.close_scan();
      return RC::CONSTRAINT_UNIQUE;
    }
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  } else {
    LOG_ERROR("failed to scan record. file id=%d, rc=%d:%s", file_id_, rc, strrc(rc));
  }

  scanner.close_scan();

  
  
  return rc;
}


RC Table::update_record(Trx *trx, const char *attribute_name, const Value *value, int condition_num, const Condition *conditions, int *updated_count) {
  CompositeConditionFilter condition_filter;
  RC rc = condition_filter.init(*this, conditions, condition_num);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 检查属性名是否合法
  const FieldMeta *field_meta = this->table_meta().field(attribute_name);
  if (nullptr == field_meta) {
    LOG_WARN("No such field. %s.%s", this->name(), attribute_name);
    return RC::SCHEMA_FIELD_MISSING;
  }
  if (field_type_compare_compatible_table(field_meta->type(), value->type) != RC::SUCCESS )
  {
    LOG_WARN("Invalid value type. field name=%s, type=%d, but given=%d", this->name(), field_meta->type(), value->type);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  if (field_meta->type() == DATES && value->type == CHARS)
  {
    char *new_val = (char *) value->data;
    Date date;
    RC rc = date.readAndValidate(new_val);
    if (rc == RC::SCHEMA_FIELD_TYPE_MISMATCH)
    {
      LOG_WARN("Invalid value type. field name=%s, type=%d, but given=%d", this->name(), field_meta->type(), value->type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    date.writeToChars(new_val);
  }

  // 如果是unique属性，先检查有没有重复的
  if (field_meta->unique())
  {
    rc = check_unique(field_meta, value);
    if (rc != RC::SUCCESS)
    {
      return rc;
    }
    
  }
  


  RecordUpdater updater(trx, field_meta,*this, value);
  rc = scan_record(trx, &condition_filter, -1, &updater, record_reader_update_adapter);
  if (updated_count != nullptr)
  {
    *updated_count = updater.updated_count();
  }
  return rc;
}
/*
RC Table::update_record(Trx *trx, Record *record) {
  RC rc = RC::SUCCESS;
  if (trx != nullptr) {
    rc = trx->delete_record(this, record);
  } else {
    rc = delete_entry_of_indexes(record->data, record->rid, false);// 重复代码 refer to commit_delete
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to delete indexes of record (rid=%d.%d). rc=%d:%s",
                record->rid.page_num, record->rid.slot_num, rc, strrc(rc));
    } else {
      rc = record_handler_->delete_record(&record->rid);
    }
  }
  return rc;
}
*/

class RecordDeleter {
public:
  RecordDeleter(Table &table, Trx *trx) : table_(table), trx_(trx) {
  }

  RC delete_record(Record *record) {
    RC rc = RC::SUCCESS;
    rc = table_.delete_record(trx_, record);
    if (rc == RC::SUCCESS) {
      deleted_count_++;
    }
    return rc;
  }

  int deleted_count() const {
    return deleted_count_;
  }

private:
  Table & table_;
  Trx *trx_;
  int deleted_count_ = 0;
};

static RC record_reader_delete_adapter(Record *record, void *context) {
  RecordDeleter &record_deleter = *(RecordDeleter *)context;
  return record_deleter.delete_record(record);
}

RC Table::delete_record(Trx *trx, ConditionFilter *filter, int *deleted_count) {
  
  RecordDeleter deleter(*this, trx);
  RC rc = scan_record(trx, filter, -1, &deleter, record_reader_delete_adapter);
  if (deleted_count != nullptr) {
    *deleted_count = deleter.deleted_count();
  }
  return rc;
}

RC Table::delete_record(Trx *trx, Record *record) {
  RC rc = RC::SUCCESS;
  if (trx != nullptr) {
    rc = trx->delete_record(this, record);
    if(rc != RC::SUCCESS){
      return rc;
    }
  }
  else{
    rc = delete_entry_of_indexes(record->data, record->rid, false);// 重复代码 refer to commit_delete
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to delete indexes of record (rid=%d.%d). rc=%d:%s",
                record->rid.page_num, record->rid.slot_num, rc, strrc(rc));
    } else {
      rc = record_handler_->delete_record(&record->rid);
    }
  }
  
  
  return rc;
}

RC Table::commit_delete(Trx *trx, const RID &rid) {
  RC rc = RC::SUCCESS;
  Record record;
  rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  rc = delete_entry_of_indexes(record.data, record.rid, false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete indexes of record(rid=%d.%d). rc=%d:%s",
              rid.page_num, rid.slot_num, rc, strrc(rc));// panic?
  }

  rc = record_handler_->delete_record(&rid);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return rc;
}

RC Table::rollback_delete(Trx *trx, const RID &rid) {
  RC rc = RC::SUCCESS;
  Record record;
  rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return trx->rollback_delete(this, record); // update record in place
}

RC Table::insert_entry_of_indexes(const char *record, const RID &rid) {
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC Table::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists) {
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

Index *Table::find_index(const char *index_name) const {
  for (Index *index: indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}

IndexScanner *Table::find_index_for_scan(const DefaultConditionFilter &filter) {
  const ConDesc *field_cond_desc = nullptr;
  const ConDesc *value_cond_desc = nullptr;
  if (filter.left().is_attr && !filter.right().is_attr) {
    field_cond_desc = &filter.left();
    value_cond_desc = &filter.right();
  } else if (filter.right().is_attr && !filter.left().is_attr) {
    field_cond_desc = &filter.right();
    value_cond_desc = &filter.left();
  }
  if (field_cond_desc == nullptr || value_cond_desc == nullptr) {
    return nullptr;
  }

  const FieldMeta *field_meta = table_meta_.find_field_by_offset(field_cond_desc->attr_offset);
  if (nullptr == field_meta) {
    LOG_PANIC("Cannot find field by offset %d. table=%s",
              field_cond_desc->attr_offset, name());
    return nullptr;
  }

  const IndexMeta *index_meta = table_meta_.find_index_by_field(field_meta->name());
  if (nullptr == index_meta) {
    return nullptr;
  }

  Index *index = find_index(index_meta->name());
  if (nullptr == index) {
    return nullptr;
  }
  void * _value = (char *)malloc(field_meta->len() + sizeof(bool));
  memcpy(_value, value_cond_desc->value, field_meta->len());
  memcpy(_value + field_meta->len(), &value_cond_desc->is_null, sizeof(bool));
  return index->create_scanner(filter.comp_op(), (const char *)_value);
}

IndexScanner *Table::find_index_for_scan(const ConditionFilter *filter) {
  if (nullptr == filter) {
    return nullptr;
  }

  // remove dynamic_cast
  const DefaultConditionFilter *default_condition_filter = dynamic_cast<const DefaultConditionFilter *>(filter);
  if (default_condition_filter != nullptr) {
    return find_index_for_scan(*default_condition_filter);
  }

  const CompositeConditionFilter *composite_condition_filter = dynamic_cast<const CompositeConditionFilter *>(filter);
  if (composite_condition_filter != nullptr) {
    int filter_num = composite_condition_filter->filter_num();
    for (int i = 0; i < filter_num; i++) {
      IndexScanner *scanner= find_index_for_scan(&composite_condition_filter->filter(i));
      if (scanner != nullptr) {
        return scanner; // 可以找到一个最优的，比如比较符号是=
      }
    }
  }
  return nullptr;
}

RC Table::sync() {
  RC rc = data_buffer_pool_->flush_all_pages(file_id_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to flush table's data pages. table=%s, rc=%d:%s", name(), rc, strrc(rc));
    return rc;
  }

  for (Index *index: indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
                name(), index->index_meta().name(), rc, strrc(rc));
      return rc;
    }
  }
  LOG_INFO("Sync table over. table=%s", name());
  return rc;
}

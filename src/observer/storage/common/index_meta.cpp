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
// Created by wangyunlai.wyl on 2021/5/18.
//

#include "storage/common/index_meta.h"
#include "storage/common/field_meta.h"
#include "storage/common/table_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "rc.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");

RC IndexMeta::init(const char *name, const FieldMeta ** fields, const int attribute_num) {
  if (nullptr == name || common::is_blank(name)) {
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  for (size_t i = 0; i < attribute_num; i++) {
    fields_.push_back((*fields[i]).name());
  }
  
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const {
  json_value[FIELD_NAME] = name_;
  Json::Value fields_value;
  for (std::string field: fields_) {
    Json::Value field_value;
    field_value = field;
    fields_value.append(std::move(field_value));
  }
  json_value[FIELD_FIELD_NAME] = fields_value;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index) {
  const Json::Value &name_value = json_value[FIELD_NAME];
  const Json::Value &fields_value = json_value[FIELD_FIELD_NAME];
  if (!fields_value.isArray() || fields_value.size() <= 0) {
    LOG_ERROR("Invalid index meta. fields is not array, json value=%s", fields_value.toStyledString().c_str());
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::GENERIC_ERROR;
  }


  int field_num = fields_value.size();
  const FieldMeta *fields[field_num];
  for (int i = 0; i < field_num; i++) {// 挨个读取
    const Json::Value &field_value = fields_value[i];
    if (!field_value.isString()) {
      LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
                name_value.asCString(), field_value.toStyledString().c_str());
      return RC::GENERIC_ERROR;
    }
    const FieldMeta *field = table.field(field_value.asCString());
    if (nullptr == field) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }

    fields[i] = field;
  }

  return index.init(name_value.asCString(), fields, field_num);
}

const char *IndexMeta::name() const {
  return name_.c_str();
}

const std::vector<std::string> IndexMeta::fields() const {
  return fields_;
}

void IndexMeta::desc(std::ostream &os) const {
  os << "index name=" << name_;
  for (std::string field: fields_) {
    os << ", field=" << field;
  }
}
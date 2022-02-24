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
// Created by Wangyunlai on 2021/5/7.
//
#include <math.h>
#include <stddef.h>
#include <unordered_set>
#include "condition_filter.h"
#include "record_manager.h"
#include "common/log/log.h"
#include "storage/common/table.h"
#include "common/lang/string.h"
#include "storage/common/date.h"

using namespace common;

RC field_type_compare_compatible_table(AttrType type_left, AttrType type_right)
{
  static std::vector<std::unordered_set<AttrType>> type_map = {
      {},
      {CHARS},
      {INTS, FLOATS},
      {FLOATS,INTS},
      {DATES, CHARS},
      {CHARS, TEXTS},
      {INTS, FLOATS, CHARS, DATES, NONE}};
  auto &left_compatitable_set = type_map[type_left];
  auto &right_compatitable_set = type_map[type_right];
  if ((left_compatitable_set.find(type_right) == left_compatitable_set.end()) && (right_compatitable_set.find(type_left) == right_compatitable_set.end()))
  {
    LOG_ERROR("Invalid type comparision with unsupported attribute type: %d,%d", type_left, type_right);
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
  return RC::SUCCESS;
}

ConditionFilter::~ConditionFilter()
{}

DefaultConditionFilter::DefaultConditionFilter()
{
  left_.is_attr = false;
  left_.attr_length = 0;
  left_.attr_offset = 0;
  left_.value = nullptr;
  left_.is_null = false;

  right_.is_attr = false;
  right_.attr_length = 0;
  right_.attr_offset = 0;
  right_.value = nullptr;
  right_.is_null = false;
}
DefaultConditionFilter::~DefaultConditionFilter()
{}

RC DefaultConditionFilter::init(const ConDesc &left, const ConDesc &right, AttrType attr_type_left, AttrType attr_type_right, CompOp comp_op)
{
  if (attr_type_left < CHARS || attr_type_left > NONE) {
    LOG_ERROR("Invalid condition with unsupported attribute type left: %d", attr_type_left);
    return RC::INVALID_ARGUMENT;
  }

  if (attr_type_right < CHARS || attr_type_right > NONE) {
    LOG_ERROR("Invalid condition with unsupported attribute type right: %d", attr_type_right);
    return RC::INVALID_ARGUMENT;
  }

  if (comp_op < EQUAL_TO || comp_op >= NO_OP) {
    LOG_ERROR("Invalid condition with unsupported compare operation: %d", comp_op);
    return RC::INVALID_ARGUMENT;
  }

  left_ = left;
  right_ = right;
  attr_type_left_ = attr_type_left;
  attr_type_right_ = attr_type_right;
  comp_op_ = comp_op;
  return RC::SUCCESS;
}

CompOp DefaultConditionFilter::swapOp(CompOp op) {
  if (op == CompOp::GREAT_EQUAL) {
    return CompOp::LESS_EQUAL;
  } else if (op == CompOp::GREAT_THAN) {
    return CompOp::LESS_THAN;
  } else if (op == CompOp::LESS_EQUAL) {
    return CompOp::GREAT_EQUAL;
  } else if (op == CompOp::LESS_THAN) {
    return CompOp::GREAT_THAN;
  }
  else if( op == CompOp::IS_NOT_NULL || op == CompOp::IS_NULL || op == CompOp::EQUAL_TO){
    return op;
  }
  LOG_PANIC("unsuported compare operation");
  return op;
}

RC DefaultConditionFilter::init(Table &table, const Condition &condition)
{
  const TableMeta &table_meta = table.table_meta();
  ConDesc left;
  ConDesc right;

  AttrType type_left = UNDEFINED;
  AttrType type_right = UNDEFINED;

  if (1 == condition.left_is_attr) {
    left.is_attr = true;
    const FieldMeta *field_left = table_meta.field(condition.left_attr.attribute_name);
    if (nullptr == field_left) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), condition.left_attr.attribute_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    left.attr_length = field_left->len() + sizeof(bool);
    left.attr_offset = field_left->offset();

    left.value = nullptr;
    left.is_null = false;

    type_left = field_left->type();
  } else {
    left.is_attr = false;
    left.value = condition.left_value.data;  // 校验type 或者转换类型
    type_left = condition.left_value.type;
    left.is_null = condition.left_value.type == NONE;

    left.attr_length = 0;
    left.attr_offset = 0;
  }

  if (1 == condition.right_is_attr) {
    right.is_attr = true;
    const FieldMeta *field_right = table_meta.field(condition.right_attr.attribute_name);
    if (nullptr == field_right) {
      LOG_WARN("No such field in condition. %s.%s", table.name(), condition.right_attr.attribute_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    right.attr_length = field_right->len() + sizeof(bool);
    right.attr_offset = field_right->offset();
    type_right = field_right->type();
    right.is_null = false;

    right.value = nullptr;
  } else {
    right.is_attr = false;
    right.value = condition.right_value.data;
    type_right = condition.right_value.type;
    right.is_null = condition.right_value.type == NONE;

    right.attr_length = 0;
    right.attr_offset = 0;
  }
  // 校验和转换
  RC rc= field_type_compare_compatible_table(type_left, type_right);
  if (rc != RC::SUCCESS) {
     // 不能比较的两个字段， 要把信息传给客户端
     return rc;
  }
  // NOTE：这里没有实现不同类型的数据比较，比如整数跟浮点数之间的对比
  // 但是选手们还是要实现。这个功能在预选赛中会出现
  AttrType type;
  Date date;
  CompOp op = condition.comp;
  if (!left.is_attr && right.is_attr) {  // left:value, right:attr
    type = type_right;
    if (type == DATES && !left.is_null) {
      rc = date.readAndValidate(reinterpret_cast<char *>(left.value)); // validate left value
      date.writeToChars(reinterpret_cast<char *>(left.value));
    }
    std::swap(left, right);
    std::swap(type_left, type_right);
    op = swapOp(op);
  } else if (left.is_attr && !right.is_attr) {  // left:attr, right:value
    type = type_left;
    if (type == DATES&& !right.is_null) {
      rc = date.readAndValidate(reinterpret_cast<char *>(right.value)); // validate right value
      date.writeToChars(reinterpret_cast<char *>(right.value));
    }
  } else if (left.is_attr && right.is_attr) {  // both attr   // TODO
    type = type_left;
  } else {  // both value
    type = type_left;
  }
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // return init(left, right, type, op);
  return init(left, right, type_left, type_right, op);
}

bool DefaultConditionFilter::filter(const Record &rec) const
{
  char *left_value = nullptr;
  char *right_value = nullptr;

  if (comp_op_ == IS_NULL || comp_op_ == IS_NOT_NULL) {
    if (left_.is_attr){
      return *(bool *) (rec.data + left_.attr_offset + left_.attr_length - sizeof(bool)) ? comp_op_ == IS_NULL : comp_op_ == IS_NOT_NULL;
    }
    else{
      return left_.is_null ? comp_op_ == IS_NULL : comp_op_ == IS_NOT_NULL;
    }
  }
  

  if (left_.is_attr) { // value
    left_value = reinterpret_cast<char *>(rec.data + left_.attr_offset);
    if (*(bool *) (rec.data + left_.attr_offset + left_.attr_length - sizeof(bool))) {
      return false;
    }
  }
  else {
    left_value = reinterpret_cast<char *>(left_.value);
    if (left_.is_null) {
      return false;
    }
  }

  if (right_.is_attr) {
    right_value = reinterpret_cast<char *>(rec.data + right_.attr_offset);
    if (*(bool *) (rec.data + right_.attr_offset + right_.attr_length - sizeof(bool))) {
      return false;
    }
  }
  else {
    right_value = reinterpret_cast<char *>(right_.value);
    if (right_.is_null) {
      return false;
    }
  }

  int cmp_result = 0;
  switch (attr_type_left_) {        // ! 针对CHARS和Dates不变
    case CHARS: { 
      cmp_result = strcmp(left_value, right_value);
    } break;
    case INTS:  {
      int left = *reinterpret_cast<int *>(left_value), right = 0;
      if (attr_type_right_ == INTS)
        right = *reinterpret_cast<int *>(right_value);
      else if (attr_type_left_ == FLOATS)
        right = ceil(*reinterpret_cast<float *>(right_value));
      else LOG_PANIC("Not a valid type comparison");
      cmp_result = left - right;
    } break;
    case FLOATS:  {
      float left = *reinterpret_cast<float *>(left_value), right = 0.0;
      if (attr_type_right_ == INTS)
        right = float(*reinterpret_cast<int *>(right_value));
      else if (attr_type_left_ == FLOATS)
        right = *reinterpret_cast<float *>(right_value);
      else LOG_PANIC("Not a valid type comparison");
      cmp_result = (left == right) ? 0 : (left < right ? -1: 1);
    } break;
    case DATES: {
      Date l_date, r_date;
      // TODO right values may not need to be parsed repeatedly
      if (l_date.readAndValidate(left_value) == RC::SUCCESS && r_date.readAndValidate(right_value) == RC::SUCCESS)  {
        cmp_result = l_date.compare(r_date);
      }
      else  {
        LOG_PANIC("Never should print this for dates.");
      }
    } break;
    default:  {
      LOG_PANIC("Not a valid type");
    }
  }

  switch (comp_op_)
  {
  case EQUAL_TO:
    return 0 == cmp_result;
  case LESS_EQUAL:
    return cmp_result <= 0;
  case NOT_EQUAL:
    return cmp_result != 0;
  case LESS_THAN:
    return cmp_result < 0;
  case GREAT_EQUAL:
    return cmp_result >= 0;
  case GREAT_THAN:
    return cmp_result > 0;

  default:
    break;
  }

  LOG_PANIC("Never should print this.");
  return cmp_result;  // should not go here
}

CompositeConditionFilter::~CompositeConditionFilter()
{
  if (memory_owner_) {
    delete[] filters_;
    filters_ = nullptr;
  }
}

RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num, bool own_memory)
{
  filters_ = filters;
  filter_num_ = filter_num;
  memory_owner_ = own_memory;
  return RC::SUCCESS;
}
RC CompositeConditionFilter::init(const ConditionFilter *filters[], int filter_num)
{
  return init(filters, filter_num, false);
}

RC CompositeConditionFilter::init(Table &table, const Condition *conditions, int condition_num)
{
  if (condition_num == 0) {
    return RC::SUCCESS;
  }
  if (conditions == nullptr) {
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;
  ConditionFilter **condition_filters = new ConditionFilter *[condition_num];
  for (int i = 0; i < condition_num; i++) {
    DefaultConditionFilter *default_condition_filter = new DefaultConditionFilter();
    rc = default_condition_filter->init(table, conditions[i]);
    if (rc != RC::SUCCESS) {
      delete default_condition_filter;
      for (int j = i - 1; j >= 0; j--) {
        delete condition_filters[j];
        condition_filters[j] = nullptr;
      }
      delete[] condition_filters;
      condition_filters = nullptr;
      return rc;
    }
    condition_filters[i] = default_condition_filter;
  }
  return init((const ConditionFilter **)condition_filters, condition_num, true);
}

bool CompositeConditionFilter::filter(const Record &rec) const
{
  for (int i = 0; i < filter_num_; i++) {
    if (!filters_[i]->filter(rec)) {
      return false;
    }
  }
  return true;
}

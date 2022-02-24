/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/14.
//

#ifndef __OBSERVER_SQL_EXECUTOR_VALUE_H_
#define __OBSERVER_SQL_EXECUTOR_VALUE_H_

#include <string.h>
#include "common/io/io.h"
#include <math.h>
#include <string>
#include <ostream>
#include "storage/common/date.h"

class TupleValue {
public:
  TupleValue() = default;
  virtual ~TupleValue() = default;

  virtual void to_string(std::ostream &os) const = 0;
  virtual int compare(const TupleValue &other) const = 0;
  const bool operator==(const TupleValue &value )const;
  size_t hash() const;
  bool set_null(bool new_null){
    null_ = new_null;
  }

  bool is_null()const{
    return null_;
  }
  bool null_ = false;

};

class NumericalValue: public TupleValue {
public:
    virtual void to_string(std::ostream &os) const = 0;
    virtual int compare(const TupleValue &other) const = 0;
    virtual double get_value() const = 0;
};

class FloatValue : public NumericalValue {
public:
  typedef float value_type;
  explicit FloatValue(float value, bool is_null = false) : value_(value) {
    null_ = is_null;
  }

  FloatValue(std::string value, bool is_null = false) : value_ (atof(value.c_str())) {
    null_ = is_null;
  }

  void to_string(std::ostream &os) const override {
    if (this->is_null()){
      os << "NULL";
      return ;
    }
    
    int integer = int(value_);
    int frac = int(round( (value_+1e-7)*100)) %100;
    os << frac * 0.01 + integer;
  }

  int compare(const TupleValue &other) const override {
    const FloatValue & float_other = (const FloatValue &)other;
    float result = value_ - float_other.value_;
    if (result > 0) { // 浮点数没有考虑精度问题
      return 1;
    }
    if (result < 0) {
      return -1;
    }
    return 0;
  }
  
  const bool operator==(const TupleValue &other )const {
    const FloatValue & float_other = (const FloatValue &)other;
    return value_ == float_other.value_;
  }

  const void add(FloatValue &other) {
    const FloatValue &float_other = (const FloatValue &)other;
    this->value_ += float_other.value_;
  }

  std::size_t hash() const {
    return std::hash<float>()(value_);
  }

public:
 double get_value() const { return (double) this->value_; }
 void set_value(float value) { this->value_ = value; }

private:
  float value_;

};

class IntValue : public NumericalValue {
public:
  typedef int value_type;
  explicit IntValue(int value, bool is_null = false) : value_(value) {
    null_ = is_null;
  }

  void to_string(std::ostream &os) const override {
    if (this->is_null()){
      os << "NULL";
      return ;
    }
    os << value_;
  }
  int compare(const TupleValue &other) const override {
    const IntValue & int_other = (const IntValue &)other;
    const FloatValue &float_other = (const FloatValue &)other;
    int compare_val;
    if (float_other.get_value() >= 0 && float_other.get_value() <= 0.000001)    
      compare_val = int_other.value_;
    else {   // ! 这种情况只会出现在avg里，因为avg默认存的都是FloatValue
      double right = float_other.get_value();
      return (double) value_ == right ? 0 : ((double) value_ > right ? 1 : -1);
    }       
    return value_ == compare_val? 0 : (value_ > compare_val? 1 : -1);
  }
  const bool operator==(const TupleValue &other )const {
    const IntValue & int_other = (const IntValue &)other;
    return value_ == int_other.value_;
  }

  const void add(IntValue &other) {
    const IntValue &int_other = (const IntValue &)other;
    this->value_ += int_other.value_;
  }

  std::size_t hash() const {
    return std::hash<int>()(value_);
  }

 double get_value() const { return (double) this->value_; }
 void set_value(int value) { this->value_ = value; }

 private:
  int value_;
};

class TextValue : public TupleValue {
public:
  typedef int value_type;
  explicit TextValue(int value, const std::string table_name, bool is_null = false) : value_(value),table_name_(table_name) {
    null_ = is_null;
  }

  void to_string(std::ostream &os) const override {
    if (this->is_null()){
      os << "NULL";
      return ;
    }
    std::string file_name =  table_name_ + "-test-" + std::to_string(value_);
    char * text;
    size_t text_size = 4096;
    common::readFromFile(file_name, text, text_size);
    if(text_size == 4096){
      std::string result(text, 4096);
      os << result;
    } else {
      os << text;
    }
  }
  int compare(const TupleValue &other) const override {
    return false;
  }
  const bool operator==(const TupleValue &other )const {
    const TextValue & text_other = (const TextValue &)other;
    return value_ == text_other.value_;
  }

  const void add(TupleValue &other) {
  }

  std::size_t hash() const {
    return std::hash<int>()(value_);
  }

 const int get_value() const { return this->value_; }
 void set_value(int value) { this->value_ = value; }

 private:
  int value_;
  const std::string table_name_;
};

class StringValue : public TupleValue {
public:
  typedef std::string value_type;
  explicit StringValue(const char *value, int len, bool is_null = false) : value_(value, len){
    null_ = is_null;
  }
  explicit StringValue(const char *value, bool is_null = false) : value_(value) {
    null_ = is_null;
  }
  explicit StringValue(std::string value, bool is_null = false) : value_(value) {
    null_ = is_null;
  }
  explicit StringValue(float value, bool is_null = false) : value_(std::to_string(value)) {
    null_ = is_null;
  }

  void to_string(std::ostream &os) const override {
    if (this->is_null()){
      os << "NULL";
      return ;
    }
    os << value_;
  }

  int compare(const TupleValue &other) const override {
    const StringValue &string_other = (const StringValue &)other;
    return strcmp(value_.c_str(), string_other.value_.c_str());
  }

  const bool operator==(const TupleValue &other )const {
    const StringValue & string_other = (const StringValue &)other;
    return strcmp(value_.c_str(), string_other.value_.c_str()) == 0;
  }

  const void add(StringValue &other) { }

  std::size_t hash() const {
    return std::hash<std::string>()(value_);
  }
  
public:
 void set_value(const std::string& value) { this->value_ = value; }
  std::string get_value() { return this->value_; }
  

private:
  std::string value_;
};

class TupleValueHash {
public:
    std::size_t operator()(const TupleValue &p) const {
        return p.hash();
    }
};
#endif //__OBSERVER_SQL_EXECUTOR_VALUE_H_

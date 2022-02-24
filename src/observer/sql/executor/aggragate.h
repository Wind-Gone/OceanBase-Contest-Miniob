//
// Created by Hu on 2021/10/19.
//

#ifndef __OBSERVER_SQL_EXECUTE_AGGRAGATE_H__
#define __OBSERVER_SQL_EXECUTE_AGGRAGATE_H__
#include "sql/parser/parse_defs.h"
#include "storage/common/table_meta.h"
#include "sql/executor/tuple.h"

class Aggragate {
 public:
  Aggragate() = default;
  virtual ~Aggragate() = default;
  virtual void execute(std::vector<TupleSet> tuple_sets, char* field_name);
};

template <class T>
class MaxAggr : public Aggragate {
 public:
  MaxAggr(T value) { this->value = value; }
  void execute(std::vector<TupleSet> tuple_sets, char* field_name) {}
  T get_result() const { return value; }

 private:
  T value;
};

template <class T>
class MinAggr : public Aggragate {
 public:
  MinAggr(T value) { this->value = value; }
  void execute(std::vector<TupleSet> tuple_sets, char* field_name) {}
  T get_result() const { return value; }

 private:
  T value;
};

template <class T>
class AvgAggr : public Aggragate {
 public:
  AvgAggr(T value) { this->value = value; }

  void execute(std::vector<TupleSet> tuple_sets, char* field_name) {}
  T get_result() const { return value; }

 private:
  T value;
};

class CountAggr : public Aggragate {
 public:
  void execute(std::vector<TupleSet> tuple_sets, char* field_name) {}
  int get_result() const { return value; }

 private:
  int value;
};

#endif  // __OBSERVER_STORAGE_COMMON_TABLE_H__

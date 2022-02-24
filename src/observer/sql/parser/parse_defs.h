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
// Created by wangyunlai.wyl on 2021/6/7.
//

#ifndef __OBSERVER_SQL_PARSER_PARSE_DEFS_H__
#define __OBSERVER_SQL_PARSER_PARSE_DEFS_H__

#include <stddef.h>
#include <string.h>

#define MAX_NUM 20
#define MAX_REL_NAME 20
#define MAX_ATTR_NAME 20
#define MAX_ERROR_MESSAGE 20
#define MAX_DATA 50
// 不同数据类型的默认存储长度
static const size_t kCharsDefaultLength = 4;
static const size_t kDateDefaultLength = 12;
static const size_t kIntDefaultLength = 4;
static const size_t kFloatDefaultLength = 4;

// * 聚合值类型
typedef enum { MAX, MIN, AVG, COUNT, UNDEFINEDAGGR } AggrType;
//属性结构体
typedef struct _RelAttr {
  char *relation_name;   // relation name (may be NULL) 表名
  char *attribute_name;  // attribute name              属性名
  AggrType aggr_type;     // 聚合算子类型
  float aggr_value;      // 算子值(如果是纯数字的话)
  void *expr;             // expression
} RelAttr;

typedef enum {
  EQUAL_TO,     //"="     0
  LESS_EQUAL,   //"<="    1
  NOT_EQUAL,    //"<>"    2
  LESS_THAN,    //"<"     3
  GREAT_EQUAL,  //">="    4
  GREAT_THAN,   //">"     5
  IN,           // "IN"   6
  NOT_IN,       // "NOT_IN"   7
  IS_NULL,      // "IS NULL"  8
  IS_NOT_NULL,  // "IS NOT NULL" 9
  NO_OP
} CompOp;

//属性值类型
typedef enum { UNDEFINED, CHARS, INTS, FLOATS, DATES, TEXTS, NONE, ARITHMETIC_EXPR } AttrType;
typedef enum { PLUS = 0, MINUS = 1, DIVIDE = 2, MULTIPLY = 3, INT_VALUE_EXPR = 4, FLOAT_VALUE_EXPR = 5, STR_VALUE_EXPR = 6, NULL_VALUE_EXPR = 7, BRACE_EXPR = 8, IDENTIFIER_EXPR = 9, ENSURE_SIZE = 0x7fffffffffffffffUL} ExprType; // make sure arithmetic op is the same size as pointer

struct OpNode {
  ExprType type;
  void* left;
  void* right;
};

struct IdentifierNode {
  ExprType type;
  char* table_name;
  char* attribute_name;
};

struct NumericalValueNode {
  ExprType type;
  double value;
};

struct StringValueNode {
  ExprType type;
  char *value;
};

struct BraceNode {
  ExprType type;
  void *expr;
};


//属性值
typedef struct _Value {
  AttrType type;  // type of value
  void *data;     // value
} Value;

typedef struct _AggrValue {
  float aggr_value;
  char *relation_name;
  char *attribute_name;
} AggrValue;

typedef struct _Condition {
  int left_is_attr;    // TRUE if left-hand side is an attribute
                       // 1时，操作符左边是属性名，0时，是属性值
  Value left_value;    // left-hand side value if left_is_attr = FALSE
  RelAttr left_attr;   // left-hand side attribute
  CompOp comp;         // comparison operator
  int right_is_attr;   // TRUE if right-hand side is an attribute
                       // 1时，操作符右边是属性名，0时，是属性值
  RelAttr right_attr;  // right-hand side attribute if right_is_attr = TRUE 右边的属性
  Value right_value;   // right-hand side value if right_is_attr = FALSE
} Condition;


typedef struct _Conditions {
  size_t condition_length;
  Condition conditions[MAX_NUM];
} Conditions;

typedef struct _CompOps {
  CompOp comp;
} CompOps;

typedef struct _Values {
  size_t value_length;
  Value values[MAX_NUM];
} Values;

typedef struct _Order {
  RelAttr attribute;
  size_t asc;
} Order;

typedef struct _Group {
  RelAttr attribute;
} Group;

// struct of select
typedef struct _Selects{
  size_t    attr_num;               // Length of attrs in Select clause
  RelAttr   attributes[MAX_NUM];    // attrs in Select clause
  size_t    relation_num;           // Length of relations in Fro clause
  char *    relations[MAX_NUM];     // relations in From clause
  size_t    condition_num;          // Length of conditions in Where clause
  Condition conditions[MAX_NUM];    // conditions in Where clause
  size_t    order_num;              // ? Number of orders
  Order     orders[MAX_NUM];        // ? Orders in Select clause
  size_t    group_num;              // ? Number of groups
  Group     groups[MAX_NUM];        // ? Groups in Select clause
  size_t    sub_num;
} Selects;

typedef struct {
  size_t value_num;       // Length of values
  Value values[MAX_NUM];  // values to insert
} InsertTuple;

// struct of insert
typedef struct {
  char *relation_name;    // Relation to insert into
  size_t tuple_num;
  InsertTuple tuples[MAX_NUM];
} Inserts;

// struct of delete
typedef struct {
  char *relation_name;            // Relation to delete from
  size_t condition_num;           // Length of conditions in Where clause
  Condition conditions[MAX_NUM];  // conditions in Where clause
} Deletes;

// struct of update
typedef struct {
  char *relation_name;            // Relation to update
  char *attribute_name;           // Attribute to update
  Value value;                    // update value
  size_t condition_num;           // Length of conditions in Where clause
  Condition conditions[MAX_NUM];  // conditions in Where clause
} Updates;

typedef struct {
  char *name;     // Attribute name
  AttrType type;  // Type of attribute
  size_t length;  // Length of attribute
  int nullable;
} AttrInfo;

// struct of craete_table
typedef struct {
  char *relation_name;           // Relation name
  size_t attribute_count;        // Length of attribute
  AttrInfo attributes[MAX_NUM];  // attributes
} CreateTable;

// struct of drop_table
typedef struct {
  char *relation_name;  // Relation name
} DropTable;

// struct of create_index
typedef struct {
  char *index_name;      // Index name
  char *relation_name;   // Relation name
  char *attribute_name[MAX_NUM];  // Attribute name
  int attribute_num;
  int unique; // 0:not unique;1: unique
} CreateIndex;

// struct of  drop_index
typedef struct {
  const char *index_name;  // Index name
  const char *table_name;
} DropIndex;

typedef struct {
  const char *relation_name;
} DescTable;

typedef struct {
  const char *relation_name;
  const char *file_name;
} LoadData;

union Queries {
  Selects selection[MAX_NUM];
  Inserts insertion;
  Deletes deletion;
  Updates update;
  CreateTable create_table;
  DropTable drop_table;
  CreateIndex create_index;
  DropIndex drop_index;
  DescTable desc_table;
  LoadData load_data;
  char *errors;
};

// 修改yacc中相关数字编码为宏定义
enum SqlCommandFlag {
  SCF_ERROR = 0,
  SCF_SELECT,
  SCF_INSERT,
  SCF_UPDATE,
  SCF_DELETE,
  SCF_CREATE_TABLE,
  SCF_DROP_TABLE,
  SCF_CREATE_INDEX,
  SCF_DROP_INDEX,
  SCF_SYNC,
  SCF_SHOW_TABLES,
  SCF_DESC_TABLE,
  SCF_BEGIN,
  SCF_COMMIT,
  SCF_ROLLBACK,
  SCF_LOAD_DATA,
  SCF_HELP,
  SCF_EXIT
};
// struct of flag and sql_struct
typedef struct Query {
  enum SqlCommandFlag flag;
  union Queries sstr;
  size_t select_num;
} Query;

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

RelAttr *relation_attr_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name);
void aggr_relation_attr_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name, float number, char *aggr_type); // ! 聚合函数初始化
void relation_attr_destroy(RelAttr *relation_attr);

void value_init_integer(Value *value, int v);
void value_init_float(Value *value, float v);
void value_init_null(Value *value);
void value_init_string(Value *value, const char *v);
void value_destroy(Value *value);
ExprType get_type(void *node);
double get_numerical_value(void *node);
const char *get_str_value(void *node);
char *get_table_name(void *node);
char *get_attribute_name(void *node);
void *create_int_expr(int val);
void *create_null_expr();
void *create_float_expr(double val);
void *create_str_expr(const char *str);
void *create_identifier_expr(char *table_name, char *attribute_name);
void *create_op_expr(ExprType op, void *left, void *right);
void *create_brace_expr(void* expr);
int is_value_expr(void* expr);
void destroy_expr(void* expr);
Value *init_value_from_expr(Value *value, void *expr);
void tuple_destroy(InsertTuple *tuple);

void condition_init(Condition *condition, CompOp comp, int left_is_attr, RelAttr *left_attr, Value *left_value,
    int right_is_attr, RelAttr *right_attr, Value *right_value);
void condition_destroy(Condition *condition);

void selects_append_order(Selects *selects, RelAttr *rel_attr, int asc);
void order_destory(Order *order);

void selects_append_group(Selects *selects, RelAttr *rel_attr);
void group_destory(Group *group);

void attr_info_init(AttrInfo *attr_info, const char *name, AttrType type, size_t length, const int nullable);
void default_attr_info_init(AttrInfo *attr_info, const char *name, AttrType type, const int nullable);
void attr_info_destroy(AttrInfo *attr_info);

void selects_init(Selects *selects, ...);
void selects_append_attribute(Selects *selects, RelAttr *rel_attr);
void selects_append_relation(Selects *selects, const char *relation_name);
// ! 增加聚合算子的函数声明
void selects_append_aggragation(Selects *selects, RelAttr *rel_aggr);
void selects_append_conditions(Selects *selects, Condition conditions[], size_t condition_num);
void selects_destroy(Selects *selects);

void inserts_init(Inserts *inserts, const char *relation_name, InsertTuple tuples[], size_t tuple_num);
void inserts_destroy(Inserts *inserts);

void deletes_init_relation(Deletes *deletes, const char *relation_name);
void deletes_set_conditions(Deletes *deletes, Condition conditions[], size_t condition_num);
void deletes_destroy(Deletes *deletes);

void updates_init(Updates *updates, const char *relation_name, const char *attribute_name, Value *value,
    Condition conditions[], size_t condition_num);
void updates_destroy(Updates *updates);

void create_table_append_attribute(CreateTable *create_table, AttrInfo *attr_info);
void create_table_init_name(CreateTable *create_table, const char *relation_name);
void create_table_destroy(CreateTable *create_table);

void drop_table_init(DropTable *drop_table, const char *relation_name);
void drop_table_destroy(DropTable *drop_table);

void create_index_init(
    CreateIndex *create_index, const char *index_name, const char *relation_name);
void create_unique_index_init(
    CreateIndex *create_index, const char *index_name, const char *relation_name);
void create_index_destroy(CreateIndex *create_index);
void index_append_attribute(CreateIndex *create_index, const char *relation_name);

void drop_index_init(DropIndex *drop_index, const char *index_name, const char *table_name);
void drop_index_destroy(DropIndex *drop_index);

void desc_table_init(DescTable *desc_table, const char *relation_name);
void desc_table_destroy(DescTable *desc_table);

void load_data_init(LoadData *load_data, const char *relation_name, const char *file_name);
void load_data_destroy(LoadData *load_data);

void query_init(Query *query);
Query *query_create();  // create and init
void query_reset(Query *query);
void query_destroy(Query *query);  // reset and delete

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // __OBSERVER_SQL_PARSER_PARSE_DEFS_H__
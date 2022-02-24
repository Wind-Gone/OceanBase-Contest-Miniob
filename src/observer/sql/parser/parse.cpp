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
// Created by Longda on 2021/4/13.
//

#include <mutex>
#include "sql/parser/parse.h"
#include "rc.h"
#include "common/log/log.h"

RC parse(char *st, Query *sqln);

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus
RelAttr *relation_attr_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name) {
  if (relation_attr == nullptr) relation_attr = (RelAttr*) malloc(sizeof(RelAttr));
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
  relation_attr->aggr_type = UNDEFINEDAGGR;
  relation_attr->aggr_value = -1;
  return relation_attr;
}

void aggr_relation_attr_init(RelAttr *relation_attr, const char *relation_name,
                             const char *attribute_name, float number,
                             char *aggr_type) {
  static std::map<std::string, AggrType> String_to_AggrType = {
      {"MAX", AggrType::MAX},
      {"MIN", AggrType::MIN},
      {"AVG", AggrType::AVG},
      {"COUNT", AggrType::COUNT},
  };
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  if (attribute_name != nullptr) {
    relation_attr->attribute_name = strdup(attribute_name);
  } else {
    relation_attr->attribute_name = nullptr;
  }
  relation_attr->aggr_value = number;  // ! 赋值，如果是数字的话
  relation_attr->aggr_type =
      String_to_AggrType[std::string(aggr_type)];  // ! 赋给聚合算子的类型
}
void relation_attr_destroy(RelAttr *relation_attr) {
  free(relation_attr->relation_name);
  free(relation_attr->attribute_name);
  relation_attr->relation_name = nullptr;
  relation_attr->attribute_name = nullptr;
}

void value_init_integer(Value *value, int v) {
  value->type = INTS;
  value->data = malloc(sizeof(v));
  memcpy(value->data, &v, sizeof(v));
}
void value_init_float(Value *value, float v) {
  value->type = FLOATS;
  value->data = malloc(sizeof(v));
  memcpy(value->data, &v, sizeof(v));
}
void value_init_null(Value *value) {
  value->type = NONE;
  value->data = malloc(sizeof(int));
}
void value_init_string(Value *value, const char *v) {
  value->type = CHARS;
  value->data = strdup(v);
}

void value_destroy(Value *value) {
  if (value->type != ARITHMETIC_EXPR) {
    free(value->data);
    value->data = nullptr;
  } else {
    destroy_expr(value->data);
    value->data = nullptr;
  }
  value->type = UNDEFINED;
}

ExprType get_type(void* node) {
  return *(ExprType*) node;
}

double get_numerical_value(void* node) {
  return ((NumericalValueNode*) node)->value;
}

const char* get_str_value(void* node) {
  return (char*) ((StringValueNode*) node)->value;
}

char* get_table_name(void* node) {
  return ((IdentifierNode*) node)->table_name;
}

char* get_attribute_name(void* node) {
  return ((IdentifierNode*) node)->attribute_name;
}

void* create_int_expr(int val) {
  NumericalValueNode* int_expr = (NumericalValueNode*) malloc(sizeof(NumericalValueNode));
  int_expr->type = INT_VALUE_EXPR;
  int_expr->value = val;
  return int_expr;
}

void* create_null_expr() {
  NumericalValueNode* null_expr = (NumericalValueNode*) malloc(sizeof(NumericalValueNode));
  null_expr->type = NULL_VALUE_EXPR;
  null_expr->value = 0;
  return null_expr;
}

void* create_float_expr(double val) {
  NumericalValueNode* float_expr = (NumericalValueNode*) malloc(sizeof(NumericalValueNode));
  float_expr->type = FLOAT_VALUE_EXPR;
  float_expr->value = val;
  return float_expr;
}

void* create_str_expr(const char* str) {
  StringValueNode* str_expr = (StringValueNode*) malloc(sizeof(StringValueNode));
  str_expr->type = STR_VALUE_EXPR;
  str_expr->value = strdup(str);
  return str_expr;
}

void* create_identifier_expr(char* table_name, char* attribute_name) {
  IdentifierNode* expr = (IdentifierNode*) malloc(sizeof(IdentifierNode));
  expr->type = IDENTIFIER_EXPR;
  expr->table_name = table_name ? strdup(table_name): NULL;
  expr->attribute_name = strdup(attribute_name);
  return expr;
}

void* create_op_expr(ExprType op, void* left, void* right) {
  OpNode* op_expr = (OpNode*) malloc(sizeof(OpNode));
  op_expr->type = op;
  op_expr->left = left;
  op_expr->right = right;
  return op_expr;
}

void* create_brace_expr(void *expr) {
  BraceNode* brace_expr = (BraceNode*) malloc(sizeof(BraceNode));
  brace_expr->type = BRACE_EXPR;
  brace_expr->expr = expr;
  return brace_expr;
}

int is_value_expr(void* expr) {
  ExprType type = get_type(expr);
  if (type >= INT_VALUE_EXPR && type <= NULL_VALUE_EXPR) return true;
  return false;
}

Value *init_value_from_expr(Value *value, void *expr) {
  ExprType type = get_type(expr);
  switch (type)
  {
  case INT_VALUE_EXPR:
    value_init_integer(value, (int) get_numerical_value(expr));
    break;
  case NULL_VALUE_EXPR:
    value_init_null(value);
    break;
  case FLOAT_VALUE_EXPR:
    value_init_float(value, get_numerical_value(expr));
    break;
  case STR_VALUE_EXPR:
    value_init_string(value, get_str_value(expr));
    break;
  default:
    LOG_PANIC("Illegal expression type");
    break;
  }
  return value;
}

void destroy_expr(void* expr) {
  ExprType type = get_type(expr);
  switch (type)
  {
  case STR_VALUE_EXPR: {
    free(((StringValueNode*) expr)->value);
    free(expr);
    break;
  }
  case NULL_VALUE_EXPR:
  case INT_VALUE_EXPR:
  case FLOAT_VALUE_EXPR: {
    free(expr);
    break;
  }
  case IDENTIFIER_EXPR: {
    char *table_name = get_table_name(expr);
    if (table_name != NULL) free(table_name);
    char *attribute_name = get_attribute_name(expr);
    free(attribute_name);
    free(expr);
    break;
  }
  case PLUS:
  case MINUS:
  case MULTIPLY:
  case DIVIDE: {
    if (((OpNode*) expr)->left != NULL) {
      destroy_expr(((OpNode*) expr)->left);
    }
    if (((OpNode*) expr)->right != NULL) {
      destroy_expr(((OpNode*) expr)->right);
    }
    free(expr);
    break;
  }
  case BRACE_EXPR: {
    destroy_expr(((BraceNode*) expr)->expr);
    ((BraceNode*) expr)->expr = NULL;
    free(expr);
  }
  default:
    LOG_PANIC("Illegal expression type");
    break;
  }
}

void tuple_destroy(InsertTuple *tuple){
  for (size_t i = 0;i < tuple->value_num; ++i){
    value_destroy(&tuple->values[i]);
  }
  tuple->value_num = 0;
}

void condition_init(Condition *condition, CompOp comp, 
                    int left_is_attr, RelAttr *left_attr, Value *left_value,
                    int right_is_attr, RelAttr *right_attr, Value *right_value) {
  condition->comp = comp;
  condition->left_is_attr = left_is_attr;
  condition->right_is_attr = right_is_attr;
  if (left_attr != nullptr || left_value != nullptr) {
      if (left_is_attr) {
      condition->left_attr = *left_attr;
    } else {
      condition->left_value = *left_value;
    }
  }
  else {
    condition->left_attr.relation_name = nullptr;
    condition->left_attr.attribute_name = nullptr;
    condition->left_value.data = nullptr;
  }
  
  if (right_attr != nullptr || right_value != nullptr) {
    if (right_is_attr) {
      condition->right_attr = *right_attr;
    } else {
      condition->right_value = *right_value;
    }
  }
  else {
    condition->right_attr.relation_name = nullptr;
    condition->right_attr.attribute_name = nullptr;
    condition->right_value.data = nullptr;
  }
}
void condition_destroy(Condition *condition) {
  if (condition->left_is_attr) {
    relation_attr_destroy(&condition->left_attr);
  } else {
    value_destroy(&condition->left_value);
  }
  if (condition->right_is_attr) {
    relation_attr_destroy(&condition->right_attr);
  } else {
    value_destroy(&condition->right_value);
  }
}

void order_destory(Order *order){
  relation_attr_destroy(&order->attribute);
}

void group_destory(Group *group) {
  relation_attr_destroy(&group->attribute);
}

void attr_info_init(AttrInfo *attr_info, const char *name, AttrType type, size_t length,const int nullable) {
  attr_info->name = strdup(name);
  attr_info->type = type;
  attr_info->length = length;
  attr_info->nullable = nullable;
}
void default_attr_info_init(AttrInfo *attr_info, const char *name, AttrType type,const int nullable) {
  attr_info->name = strdup(name);
  attr_info->type = type;
  attr_info->nullable = nullable;
  switch (type)
  {
  case DATES:
    attr_info->length = kDateDefaultLength;
    break;
  case CHARS:
    attr_info->length = kCharsDefaultLength;
    break;
  case INTS:
    attr_info->length = kIntDefaultLength;
    break;
  case FLOATS:
    attr_info->length = kFloatDefaultLength;
    break;
  default:
    break;
  }
}
void attr_info_destroy(AttrInfo *attr_info) {
  free(attr_info->name);
  attr_info->name = nullptr;
}

void selects_init(Selects *selects, ...);
void selects_append_attribute(Selects *selects, RelAttr *rel_attr) {
  selects->attributes[selects->attr_num++] = *rel_attr;
}

void selects_append_relation(Selects *selects, const char *relation_name) {
  selects->relations[selects->relation_num++] = strdup(relation_name);
}

void selects_append_conditions(Selects *selects, Condition conditions[], size_t condition_num) {
  assert(condition_num <= sizeof(selects->conditions)/sizeof(selects->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    selects->conditions[i] = conditions[i];
  }
  selects->condition_num = condition_num;
}

void selects_append_group(Selects *selects, RelAttr *rel_attr) {
  selects->groups[selects->group_num++].attribute = *rel_attr;
}

void selects_append_order(Selects *selects, RelAttr *rel_attr, int asc) {
  selects->orders[selects->order_num].attribute = *rel_attr;
  selects->orders[selects->order_num++].asc = asc;
}

void selects_destroy(Selects *selects) {
  for (size_t i = 0; i < selects->attr_num; i++) {
    relation_attr_destroy(&selects->attributes[i]);
  }
  selects->attr_num = 0;

  for (size_t i = 0; i < selects->relation_num; i++) {
    free(selects->relations[i]);
    selects->relations[i] = NULL;
  }
  selects->relation_num = 0;

  for (size_t i = 0; i < selects->condition_num; i++) {
    condition_destroy(&selects->conditions[i]);
  }
  selects->condition_num = 0;

  for (size_t i = 0; i < selects->order_num; i++) {
    order_destory(&selects->orders[i]);
  }
  selects->order_num = 0;
  
  for (size_t i = 0; i < selects->group_num; i++) {
    group_destory(&selects->groups[i]);
  }
  selects->group_num = 0;
}

void inserts_init(Inserts *inserts, const char *relation_name, InsertTuple tuples[], size_t tuple_num) {
  assert(tuple_num <= sizeof(inserts->tuples)/sizeof(inserts->tuples[0]));

  inserts->relation_name = strdup(relation_name);
  for (size_t i = 0; i < tuple_num; i++) {
    inserts->tuples[i] = tuples[i];
  }
  inserts->tuple_num = tuple_num;
}
void inserts_destroy(Inserts *inserts) {
  free(inserts->relation_name);
  inserts->relation_name = nullptr;

  for (size_t i = 0; i < inserts->tuple_num; i++) {
    tuple_destroy(&inserts->tuples[i]);
  }
  inserts->tuple_num = 0;
}

void deletes_init_relation(Deletes *deletes, const char *relation_name) {
  deletes->relation_name = strdup(relation_name);
}

void deletes_set_conditions(Deletes *deletes, Condition conditions[], size_t condition_num) {
  assert(condition_num <= sizeof(deletes->conditions)/sizeof(deletes->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    deletes->conditions[i] = conditions[i];
  }
  deletes->condition_num = condition_num;
}
void deletes_destroy(Deletes *deletes) {
  for (size_t i = 0; i < deletes->condition_num; i++) {
    condition_destroy(&deletes->conditions[i]);
  }
  deletes->condition_num = 0;
  free(deletes->relation_name);
  deletes->relation_name = nullptr;
}

void updates_init(Updates *updates, const char *relation_name, const char *attribute_name,
                  Value *value, Condition conditions[], size_t condition_num) {
  updates->relation_name = strdup(relation_name);
  updates->attribute_name = strdup(attribute_name);
  updates->value = *value;

  assert(condition_num <= sizeof(updates->conditions)/sizeof(updates->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    updates->conditions[i] = conditions[i];
  }
  updates->condition_num = condition_num;
}

void updates_destroy(Updates *updates) {
  free(updates->relation_name);
  free(updates->attribute_name);
  updates->relation_name = nullptr;
  updates->attribute_name = nullptr;

  value_destroy(&updates->value);

  for (size_t i = 0; i < updates->condition_num; i++) {
    condition_destroy(&updates->conditions[i]);
  }
  updates->condition_num = 0;
}

void create_table_append_attribute(CreateTable *create_table, AttrInfo *attr_info) {
  create_table->attributes[create_table->attribute_count++] = *attr_info;
}
void create_table_init_name(CreateTable *create_table, const char *relation_name) {
  create_table->relation_name = strdup(relation_name);
}
void create_table_destroy(CreateTable *create_table) {
  for (size_t i = 0; i < create_table->attribute_count; i++) {
    attr_info_destroy(&create_table->attributes[i]);
  }
  create_table->attribute_count = 0;
  free(create_table->relation_name);
  create_table->relation_name = nullptr;
}

void drop_table_init(DropTable *drop_table, const char *relation_name) {
  drop_table->relation_name = strdup(relation_name);
}
void drop_table_destroy(DropTable *drop_table) {
  free(drop_table->relation_name);
  drop_table->relation_name = nullptr;
}

void create_index_init(CreateIndex *create_index, const char *index_name, 
                       const char *relation_name) {
  create_index->index_name = strdup(index_name);
  create_index->relation_name = strdup(relation_name);
  create_index->unique = false;
}

void create_unique_index_init(CreateIndex *create_index, const char *index_name, 
                       const char *relation_name) {
  create_index->index_name = strdup(index_name);
  create_index->relation_name = strdup(relation_name);
  create_index->unique = true;
}

void create_index_destroy(CreateIndex *create_index) {
  free(create_index->index_name);
  free(create_index->relation_name);
  for (size_t i = 0; i < create_index->attribute_num; i++) {
    free(create_index->attribute_name[i]);
    create_index->attribute_name[i] = NULL;
  }
  create_index->attribute_num = 0;

  create_index->index_name = nullptr;
  create_index->relation_name = nullptr;

}

void index_append_attribute(CreateIndex *create_index, const char *attr_name) {
   create_index->attribute_name[create_index->attribute_num++] = strdup(attr_name);
}

void drop_index_init(DropIndex *drop_index, const char *index_name, const char *table_name) {
  drop_index->index_name = strdup(index_name);
  drop_index->table_name = strdup(table_name);
}
void drop_index_destroy(DropIndex *drop_index) {
  free((char *)drop_index->index_name);
  drop_index->index_name = nullptr;
}

void desc_table_init(DescTable *desc_table, const char *relation_name) {
  desc_table->relation_name = strdup(relation_name);
}

void desc_table_destroy(DescTable *desc_table) {
  free((char *)desc_table->relation_name);
  desc_table->relation_name = nullptr;
}

void load_data_init(LoadData *load_data, const char *relation_name, const char *file_name) {
  load_data->relation_name = strdup(relation_name);

  if (file_name[0] == '\'' || file_name[0] == '\"') {
    file_name++;
  }
  char *dup_file_name = strdup(file_name);
  int len = strlen(dup_file_name);
  if (dup_file_name[len - 1] == '\'' || dup_file_name[len - 1] == '\"') {
    dup_file_name[len - 1] = 0;
  }
  load_data->file_name = dup_file_name;
}

void load_data_destroy(LoadData *load_data) {
  free((char *)load_data->relation_name);
  free((char *)load_data->file_name);
  load_data->relation_name = nullptr;
  load_data->file_name = nullptr;
}

void query_init(Query *query) {
  query->flag = SCF_ERROR;
  memset(&query->sstr, 0, sizeof(query->sstr));
}

Query *query_create() {
  Query *query = (Query *)malloc(sizeof(Query));
  if (nullptr == query) {
    LOG_ERROR("Failed to alloc memroy for query. size=%ld", sizeof(Query));
    return nullptr;
  }

  query_init(query);
  return query;
}

void query_reset(Query *query) {
  switch (query->flag) {
    case SCF_SELECT: {
      selects_destroy(&query->sstr.selection[0]);
      selects_destroy(&query->sstr.selection[1]);
    }
    break;
    case SCF_INSERT: {
      inserts_destroy(&query->sstr.insertion);
    }
    break;
    case SCF_DELETE: {
      deletes_destroy(&query->sstr.deletion);
    }
    break;
    case SCF_UPDATE: {
      updates_destroy(&query->sstr.update);
    }
    break;
    case SCF_CREATE_TABLE: {
      create_table_destroy(&query->sstr.create_table);
    }
    break;
    case SCF_DROP_TABLE: {
      drop_table_destroy(&query->sstr.drop_table);
    }
    break;
    case SCF_CREATE_INDEX: {
      create_index_destroy(&query->sstr.create_index);
    }
    break;
    case SCF_DROP_INDEX: {
      drop_index_destroy(&query->sstr.drop_index);
    }
    break;
    case SCF_SYNC: {

    }
    break;
    case SCF_SHOW_TABLES:
    break;

    case SCF_DESC_TABLE: {
      desc_table_destroy(&query->sstr.desc_table);
    }
    break;

    case SCF_LOAD_DATA: {
      load_data_destroy(&query->sstr.load_data);
    }
    break;
    case SCF_BEGIN:
    case SCF_COMMIT:
    case SCF_ROLLBACK:
    case SCF_HELP:
    case SCF_EXIT:
    case SCF_ERROR:
    break;
  }
}

void query_destroy(Query *query) {
  query_reset(query);
  free(query);
}
#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

////////////////////////////////////////////////////////////////////////////////

extern "C" int sql_parse(const char *st, Query  *sqls);

RC parse(const char *st, Query *sqln) {
  sql_parse(st, sqln);

  if (sqln->flag == SCF_ERROR)
    return SQL_SYNTAX;
  else
    return SUCCESS;
}
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

#include <string>
#include <sstream>
#include <algorithm>

#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "event/execution_plan_event.h"
#include "sql/executor/execution_node.h"
#include "sql/executor/tuple.h"
#include "storage/common/table.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"
#include "storage/common/date.h"
#include "storage/common/condition_filter.h"

using namespace common;

RC create_selection_executor(Trx *trx, Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);
TupleSet merge_tuple_set(TupleSet &main_set,const TupleSet &join_set,std::vector<Condition> conditions);
struct OutputAttrs {
  std::vector<RelAttr> attrs;
  void addAllFields(const char* db, char* table_name) {
    Table * table_ = DefaultHandler::get_default().find_table(db, table_name);
    const TableMeta& table_meta = table_->table_meta();
    for (size_t k = table_meta.sys_field_num(); k < table_meta.field_num(); k++){
      RelAttr tmp_attr{.relation_name = table_name, .attribute_name = strdup(table_meta.field(k)->name()), .aggr_type= UNDEFINEDAGGR};
      attrs.emplace_back(tmp_attr);
    }
  }
  void addAllFields(const char* db, char* const* table_names, int num) {
    for (int j = num - 1; j >= 0; j--) {
      addAllFields(db, table_names[j]);
    }
  }
  void addField(RelAttr attr) {
    attrs.emplace_back(attr);
  }
  void addUniqueField(RelAttr attr) {
    if (std::find_if(attrs.begin(), attrs.end(), [attr](const RelAttr& lhs) {
      return ((lhs.relation_name == nullptr && attr.relation_name == nullptr) || (lhs.relation_name != nullptr && attr.relation_name != nullptr && 0 == strcmp(lhs.relation_name, attr.relation_name)))
        && ((lhs.attribute_name == nullptr && attr.attribute_name == nullptr) || (lhs.attribute_name != nullptr && attr.attribute_name != nullptr && 0 == strcmp(lhs.attribute_name, attr.attribute_name)))
        && (lhs.aggr_type == attr.aggr_type)
        && (lhs.aggr_value == attr.aggr_value)
        && (lhs.expr == attr.expr);
    }) == attrs.end())
      attrs.emplace_back(attr);
  }
};
// 获取value在tuple_set里的位置
std::vector<int> get_tuple_pos(const OutputAttrs& output_attrs, const TupleSchema& schema) {
  std::vector<int> tuple_pos;
  for (RelAttr attr:  output_attrs.attrs) {
    if (attr.attribute_name == nullptr || 0 == strcmp(attr.attribute_name,"*")) {
      tuple_pos.push_back(-1);
      continue;
    }
    
    for (int j = 0; j < schema.fields().size(); ++j) {
      TupleField mid_field = schema.fields()[j];
      if (strcmp(attr.attribute_name , mid_field.field_name()) == 0 && 
         (nullptr == attr.relation_name || 0 == strcmp(attr.relation_name,mid_field.table_name()))){
        tuple_pos.push_back(j);
        break;
      }
    }
  }
  return tuple_pos;
}
//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag) {}

//! Destructor
ExecuteStage::~ExecuteStage() {}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag) {
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties() {
  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize() {
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup() {
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event) {
  LOG_TRACE("Enter\n");

  handle_request(event);

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context) {
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.
  ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
  SQLStageEvent *sql_event = exe_event->sql_event();
  sql_event->done_immediate();

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::handle_request(common::StageEvent *event) {
  ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
  SessionEvent *session_event = exe_event->sql_event()->session_event();
  Query *sql = exe_event->sqls();
  const char *current_db = session_event->get_client()->session->get_current_db().c_str();

  CompletionCallback *cb = new (std::nothrow) CompletionCallback(this, nullptr);
  if (cb == nullptr) {
    LOG_ERROR("Failed to new callback for ExecutionPlanEvent");
    exe_event->done_immediate();
    return;
  }
  exe_event->push_callback(cb);

  switch (sql->flag) {
    case SCF_SELECT: { // select
      RC rc = do_select(current_db, sql, exe_event->sql_event()->session_event(), sql->select_num);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
      }
      exe_event->done_immediate();
    }
    break;

    case SCF_INSERT:
    case SCF_UPDATE:
    case SCF_DELETE:
    case SCF_CREATE_TABLE:
    case SCF_SHOW_TABLES:
    case SCF_DESC_TABLE:
    case SCF_DROP_TABLE:
    case SCF_CREATE_INDEX:
    case SCF_DROP_INDEX: 
    case SCF_LOAD_DATA: {
      StorageEvent *storage_event = new (std::nothrow) StorageEvent(exe_event);
      if (storage_event == nullptr) {
        LOG_ERROR("Failed to new StorageEvent");
        event->done_immediate();
        return;
      }

      default_storage_stage_->handle_event(storage_event);
    }
    break;
    case SCF_SYNC: {
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_BEGIN: {
      session_event->get_client()->session->set_trx_multi_operation_mode(true);
      session_event->set_response(strrc(RC::SUCCESS));
      exe_event->done_immediate();
    }
    break;
    case SCF_COMMIT: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->commit();
      session_event->get_client()->session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    } break;
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session_event->get_client()->session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_HELP: {
      const char *response = "show tables;\n"
          "desc `table name`;\n"
          "create table `table name` (`column name` `column type`, ...);\n"
          "create index `index name` on `table` (`column`);\n"
          "insert into `table` values(`value1`,`value2`);\n"
          "update `table` set column=value [where `column`=`value`];\n"
          "delete from `table` [where `column`=`value`];\n"
          "select [ * | `columns` ] from `table`;\n";
      session_event->set_response(response);
      exe_event->done_immediate();
    }
    break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
      exe_event->done_immediate();
    }
    break;
    default: {
      exe_event->done_immediate();
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right) {
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}

struct OrderBySort  {
  void init(int order_num,const Order* select_order,const TupleSchema schema) {
    for (int i = 0 ; i < order_num ; ++i){
      orders.push_back(select_order[i]);
      for (size_t j = 0; j < schema.fields().size(); ++j) {
        const TupleField field = schema.fields()[j];
        if ((nullptr == field.table_name() 
          || nullptr == select_order[i].attribute.relation_name 
          || 0 == strcmp(field.table_name(),select_order[i].attribute.relation_name))
          && 
            (0 == strcmp(field.field_name() , select_order[i].attribute.attribute_name))) {
          pos.push_back(j);
          break;
        }
      }
      if (pos.size() < orders.size()){ // 如果找不到对应属性，填-1，排序时跳过
        pos.push_back(-1);
      }
    }
  }
  bool operator()(const Tuple& t1, const Tuple& t2){
    for (size_t i = 0;i < orders.size(); ++i) {
      Order order = orders[i];
      int j = pos [i];
      if (j == -1){
        continue;
      }

      int res = t1.get(j).compare(t2.get(j));
      if (res != 0) {
        return (res > 0) != order.asc;// 如果t1 > t2且这个属性是升序（或其逆否），返回false
      }
    }
    return false;
  }

  bool compare(const Tuple& t1, const Tuple& t2) {
    for (size_t i = 0;i < orders.size(); ++i) {
      int j = pos[i];
      if (j == -1){
        return false;
      }
      int res = t1.get(j).compare(t2.get(j));
      if (res != 0) {
        return false;
      }
    }
    return true;
  }
  std::vector<Order> orders;
  std::vector<int> pos;
};

TupleSchema gen_schema(const Selects &selects , const char * db);
RC divide(std::vector<TupleSet> &group_tuple_sets, OrderBySort group_by, TupleSet &final_set);
RC prepare_print_for_schema(std::string &aggr_name, RelAttr attr);
RC check_select_sql(Selects & selects, const char * db);
int count_not_null_tuple(const std::vector<Tuple> &tuples, int index);
void rewrite_selects(Selects& selects, Selects& parent_selects) {
  std::unordered_set<std::string> table_sets;
  for (size_t i = 0; i < parent_selects.relation_num; i++) {
    table_sets.insert(std::string(parent_selects.relations[i]));
  }
  for (size_t i = 0; i < selects.condition_num; i++) {
    const Condition& condition = selects.conditions[i];
    if (condition.left_is_attr && condition.left_attr.relation_name != nullptr && 
      table_sets.find(std::string(condition.left_attr.relation_name)) != table_sets.end()) {
      selects.relations[selects.relation_num] = condition.left_attr.relation_name;
      selects.relation_num++;
      table_sets.insert(std::string(condition.left_attr.relation_name));
      selects.groups[selects.group_num].attribute = condition.left_attr;
      selects.group_num++;
      selects.attributes[selects.attr_num] = condition.left_attr;
      selects.attr_num ++ ;
    }
    if (condition.right_is_attr && condition.right_attr.relation_name != nullptr && 
      table_sets.find(std::string(condition.right_attr.relation_name)) != table_sets.end()) {
      selects.relations[selects.relation_num] = condition.right_attr.relation_name;
      selects.relation_num++;
      table_sets.insert(std::string(condition.right_attr.relation_name));
      selects.groups[selects.group_num].attribute = condition.right_attr;
      selects.group_num++; 
      selects.attributes[selects.attr_num] = condition.right_attr;
      selects.attr_num ++ ;
    }    
  }
}
// 这里没有对输入的某些信息做合法性校验，比如查询的列名、where条件中的列名等，没有做必要的合法性校验
// 需要补充上这一部分. 校验部分也可以放在resolve，不过跟execution放一起也没有关系
// * 执行单个sql语句查询
RC ExecuteStage::do_single_select(const char *db, Selects &selects, TupleSet &new_tuple_set, Session *session, 
  Trx *trx, bool has_expr, std::vector<int>* pos, TupleSchema *tuple_schema, Selects* parent_selects) {
  std::vector<SelectExeNode *> select_nodes;  // 执行节点
  if (parent_selects != nullptr) {
    rewrite_selects(selects, *parent_selects);
  }
  
  RC rc = check_select_sql(selects, db);  // 检查参数的合法性
  if (rc != RC::SUCCESS) {
    return rc;
  }
  
  for (size_t i = 0; i < selects.relation_num; i++) {
    const char *table_name = selects.relations[i];
    SelectExeNode *select_node = new SelectExeNode;
    rc = create_selection_executor(trx, selects, db, table_name, *select_node);
    if (rc != RC::SUCCESS) {
      delete select_node;
      for (SelectExeNode *& tmp_node: select_nodes) {
        delete tmp_node;
      }
      end_trx_if_need(session, trx, false);
      return rc;
    }
    select_nodes.push_back(select_node);
  }

  OutputAttrs output_attrs;
  bool has_aggr_flag = false;
  bool is_sub_query = false;
  for (int i = 0; i < selects.attr_num; i++) {
    const RelAttr& attr = selects.attributes[i]; 
    if (attr.aggr_type != UNDEFINEDAGGR) 
      has_aggr_flag = true;
    if (!has_aggr_flag && attr.attribute_name != nullptr && 0 == strcmp(attr.attribute_name,"*")) { // 不是聚合又是*的话，展开
      if (nullptr == attr.relation_name)
        output_attrs.addAllFields(db, selects.relations, selects.relation_num);
      else
        output_attrs.addAllFields(db, attr.relation_name);
    }
    else if (nullptr == attr.relation_name && nullptr == attr.attribute_name && attr.expr != nullptr) {
      Expr *expr = new Expr;
      RC rc = expr->init(attr.expr);
      if (rc != RC::SUCCESS) return rc;
      for (auto expr_attr: expr->getExpr()) {
        const char *attr_name = expr_attr.second[0]->attribute_name;
        const char *table_name_expr = expr_attr.second[0]->table_name == nullptr ? selects.relations[0] : expr_attr.second[0]->table_name;
        assert(table_name_expr != nullptr);
        output_attrs.addUniqueField({(char *)table_name_expr, (char *)attr_name, UNDEFINEDAGGR, -1, NULL});
      }
    }
    else {
      output_attrs.addField(attr);
    }
  }
  if (has_expr) {
    if (!(selects.attributes[0].attribute_name != nullptr && strcmp(selects.attributes[0].attribute_name, "*") == 0)) {
      for (size_t i = 0; i < selects.condition_num; i++) {
        const Condition &condition = selects.conditions[i];
        if (!condition.left_is_attr && !condition.right_is_attr && condition.left_value.type == ARITHMETIC_EXPR) {
          Expr left_expr;
          left_expr.init(condition.left_value.data);
          Expr right_expr;
          right_expr.init(condition.right_value.data);
          for (auto expr_attr: left_expr.getExpr()) {
            const char *attr_name = expr_attr.second[0]->attribute_name;
            const char *table_name_expr = expr_attr.second[0]->table_name;
            output_attrs.addUniqueField({(char *)table_name_expr, (char *)attr_name, UNDEFINEDAGGR, -1, NULL});
          }
          for (auto expr_attr: right_expr.getExpr()) {
            const char *attr_name = expr_attr.second[0]->attribute_name;
            const char *table_name_expr = expr_attr.second[0]->table_name;
            output_attrs.addUniqueField({(char *)table_name_expr, (char *)attr_name, UNDEFINEDAGGR, -1, NULL});
          }
        }
      }
    }
  }

  for (size_t i = 0; i < selects.condition_num; i++) {
      const Condition &condition = selects.conditions[i];
      if ((condition.left_is_attr && condition.right_attr.attribute_name == nullptr && condition.right_attr.relation_name == nullptr && condition.right_value.data == nullptr)
          ||
          (condition.right_is_attr && condition.left_attr.attribute_name == nullptr && condition.left_attr.relation_name == nullptr && condition.left_value.data == nullptr))
      is_sub_query = true;
  }

  if (select_nodes.empty()) {
    LOG_ERROR("No table given");
    end_trx_if_need(session, trx, false);
    return RC::SQL_SYNTAX;
  }

  std::vector<TupleSet> tuple_sets;
  for (SelectExeNode *&node: select_nodes) {
    TupleSet tuple_set;
    rc = node->execute(tuple_set);
    if (rc != RC::SUCCESS) {
      for (SelectExeNode *& tmp_node: select_nodes) {
        delete tmp_node;
      }
      end_trx_if_need(session, trx, false);
      return rc;
    } else {
      tuple_sets.push_back(std::move(tuple_set));
    }
  }

  TupleSet final_set;
  std::vector<Condition> join_conditions; // 拿到所有的join condition
  
  for (int i = 0; i < selects.condition_num; i++) {
    const Condition condition = selects.conditions[i];
    if (condition.left_is_attr == 1 && condition.right_is_attr == 1) {
      join_conditions.push_back(condition);
    }
  }
  
  // 进行join
  TupleSet& mid_set = tuple_sets[0]; 
  for (int i = 1; i < tuple_sets.size(); i++) {
    mid_set = merge_tuple_set(mid_set, tuple_sets[i], join_conditions);
  }
  final_set = std::move(mid_set);
  if (selects.order_num > 0) { //排序
    OrderBySort order_by;
    order_by.init(selects.order_num,selects.orders, final_set.get_schema());
    sort(final_set.mutable_tuples().begin(), final_set.mutable_tuples().end(), order_by);
  }

  std::vector<TupleSet> group_tuple_sets;   // * 不同range的元组的集合
  OrderBySort group_by;
  Order group_order[selects.group_num + 1];
  for (size_t i = 0; i < selects.group_num; i++) {
    group_order[i].attribute = selects.groups[i].attribute;
    group_order[i].asc = 1;
  }
  group_by.init(selects.group_num, group_order, final_set.get_schema());
  if (selects.group_num > 0) {
    sort(final_set.mutable_tuples().begin(), final_set.mutable_tuples().end(), group_by);
    divide(group_tuple_sets, group_by, final_set);    // ! 如果不是空表返回SUCCESS
  }
  else{
    TupleSet tmp_set;
    for (size_t i = 0; i < final_set.mutable_tuples().size(); i++){
      Tuple tmp_tuple;
      for (const std::shared_ptr<TupleValue> value : final_set.get(i).values()) {
        tmp_tuple.add(value);
      }
      tmp_set.add(std::move(tmp_tuple));
    }
    tmp_set.set_schema(final_set.get_schema());
    group_tuple_sets.emplace_back(std::move(tmp_set));
  }
  
  std::vector<int> tuple_pos = get_tuple_pos(output_attrs, group_tuple_sets.front().schema());
  if (has_aggr_flag)  {   // ! 有聚合的话，统一走这边，对聚合属性进行运算，普通属性则取set的第一个
    rc = group_aggr(group_tuple_sets, output_attrs.attrs, tuple_pos, new_tuple_set, selects, db);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  else  {
    if (!has_expr) {    // ! 如果没有表达式
      if (is_sub_query) {   // 如果是子查询
        for (size_t i = 0; i < final_set.tuples().size(); i++) {    
          Tuple tmp_tuple;
          for (const std::shared_ptr<TupleValue> value : final_set.get(i).values()) {
            tmp_tuple.add(value);
          }
          new_tuple_set.add(std::move(tmp_tuple));
        }
        new_tuple_set.set_schema((TupleSchema) final_set.get_schema());
      }
      else {
        for (const Tuple &tuple_ : final_set.tuples()) {    
          Tuple tmp_tuple;
          for (int i: tuple_pos) {
            tmp_tuple.add(tuple_.get_pointer(i));
          }
          new_tuple_set.add(std::move(tmp_tuple));
        }
        new_tuple_set.set_schema(gen_schema(selects, db));
      }
    }
    else {
      for (size_t i = 0; i < final_set.tuples().size(); i++) {    
        Tuple tmp_tuple;
        for (const std::shared_ptr<TupleValue> value : final_set.get(i).values()) {
          tmp_tuple.add(value);
        }
        new_tuple_set.add(std::move(tmp_tuple));
      }
      *pos = tuple_pos;
      const TupleSchema final_schema = (TupleSchema) final_set.get_schema();
      tuple_schema->append(std::move(final_schema));
      new_tuple_set.set_schema(gen_schema(selects, db));
    }
  }
  for (SelectExeNode *&tmp_node : select_nodes)
    delete tmp_node;
  return RC::SUCCESS;
}

// * 为常规比较运算符计算最终的元组集
template<typename V>
RC ExecuteStage::calculate_for_sub_query(TupleSet &fa_tuple_set, TupleSet &final_tuple_set, TupleSet &sub_tuple_set, Condition condition, const Selects &selects) {
  std::vector<Tuple> &tuples = fa_tuple_set.mutable_tuples();
  char *attr_name, *table_name;
  auto right_attr_ =  condition.right_attr,  left_attr_ =  condition.left_attr;  
  auto right_value_ = condition.right_value, left_value_ = condition.left_value;
  if (right_attr_.attribute_name == nullptr && right_attr_.relation_name == nullptr && right_value_.data == nullptr) {
    attr_name = left_attr_.attribute_name;
    table_name = (left_attr_.relation_name == nullptr) ? selects.relations[0] : left_attr_.relation_name;
  }
  else if (left_attr_.attribute_name == nullptr && left_attr_.relation_name == nullptr && left_value_.data == nullptr) {
    attr_name = right_attr_.attribute_name;
    table_name = (right_attr_.relation_name == nullptr) ? selects.relations[0] : right_attr_.relation_name;
    condition.comp = DefaultConditionFilter::swapOp(condition.comp);
  }
  else return RC::INVALID_ARGUMENT;
  for(size_t i = 0; i < tuples.size(); i++) {
    
    Tuple tmp_tuple;
    for(std::shared_ptr<TupleValue> value_: tuples[i].values()){
      tmp_tuple.add(value_);
    }
    int index = fa_tuple_set.get_schema().index_of_field(table_name, attr_name);
    if (index == -1)  index = 0;
    V& curr = dynamic_cast<V &>(tmp_tuple.get_mutable(index));
    Tuple *sub_query_tuple = &sub_tuple_set.mutable_tuples()[0];
    if (1 != sub_tuple_set.size()) {
      TupleSet tmp_set;
      tmp_set.set_schema(fa_tuple_set.get_schema());
      tmp_set.add(std::move(tmp_tuple));
      std::vector<Condition> conditions;
      for (const TupleField &field_: fa_tuple_set.get_schema().fields()) {
        Condition _condition;
        _condition.comp = EQUAL_TO;
        _condition.left_is_attr = true;
        _condition.right_is_attr = true;
        RelAttr _attr;
        _attr.attribute_name = (char *)field_.field_name();
        _attr.relation_name = (char *)field_.table_name();
        _condition.left_attr = _attr;
        _condition.right_attr = _attr;
        conditions.emplace_back(_condition);
      }
      tmp_set = merge_tuple_set(tmp_set, sub_tuple_set, conditions);
      OutputAttrs attrs;
      for (const TupleField &field_: sub_tuple_set.get_schema().fields()) {
        RelAttr _attr;
        _attr.attribute_name = (char *)field_.field_name();
        _attr.relation_name = (char *)field_.table_name();
        attrs.addField(_attr);
      }
      std::vector<int>pos = get_tuple_pos(attrs, tmp_set.schema());
      sub_query_tuple = new Tuple;
      if(tmp_set.tuples().size()){
        for(int j : pos){
            sub_query_tuple->add(tmp_set.tuples()[0].values()[j]);
        }
      }
      
    }
    
    switch (condition.comp) {
      case (EQUAL_TO) : {
        if((*sub_query_tuple).values().size() > 0)
        if (sub_tuple_set.size() != 0 && curr.compare(*(*sub_query_tuple).values()[0].get()) == 0) {
          final_tuple_set.add(std::move(tuples[i]));
        }
      } break;
      case (LESS_EQUAL) : {
       if((*sub_query_tuple).values().size() > 0)
       if (sub_tuple_set.size() != 0 && curr.compare(*(*sub_query_tuple).values()[0].get()) <= 0) {
          final_tuple_set.add(std::move(tuples[i]));
        }
      } break;
      case (NOT_EQUAL) : {
        if((*sub_query_tuple).values().size() > 0)
       if (sub_tuple_set.size() != 0 && curr.compare(*(*sub_query_tuple).values()[0].get()) != 0) {
          final_tuple_set.add(std::move(tuples[i]));
      } break;
        }
      case (LESS_THAN) : {
        if((*sub_query_tuple).values().size() > 0)
        if (sub_tuple_set.size() != 0 && curr.compare(*(*sub_query_tuple).values()[0].get()) < 0) {
          final_tuple_set.add(std::move(tuples[i]));
        }
      } break;
      case (GREAT_EQUAL) : {
        if((*sub_query_tuple).values().size() > 0)
        if (sub_tuple_set.size() != 0 && curr.compare(*(*sub_query_tuple).values()[0].get()) >= 0) {
          final_tuple_set.add(std::move(tuples[i]));
        }
      } break;
      case (GREAT_THAN) : {
        if((*sub_query_tuple).values().size() > 0)
        if (sub_tuple_set.size() != 0 && curr.compare(*(*sub_query_tuple).values()[0].get()) > 0) {
          final_tuple_set.add(std::move(tuples[i]));
        }
      } break;
      case (IN): {
        // todo check
        if (sub_tuple_set.size() != 0) {
          std::vector<Tuple> &sub_tuples = sub_tuple_set.mutable_tuples();
          char *sub_table_name = (char *) sub_tuple_set.schema().fields()[0].table_name();   // * 假定没有多表，则所有的field表名都是一样的
          for (size_t j = 0; j < sub_tuples.size(); j++) {
            Tuple &tmp_sub_tuple = sub_tuples[j];
            int sub_index = sub_tuple_set.get_schema().index_of_field(sub_table_name, attr_name);
            if (sub_index == -1)  sub_index = 0;
            V &sub_curr = dynamic_cast<V &>(tmp_sub_tuple.get_mutable(sub_index));
            if (curr.compare(sub_curr) == 0) {
              final_tuple_set.add(std::move(tuples[i]));
              break;
            }
          }
        }
      } break;
      case (NOT_IN): {
        std::vector<Tuple> &sub_tuples = sub_tuple_set.mutable_tuples();
        char *sub_table_name = (char *) sub_tuple_set.schema().fields()[0].table_name();
        char *sub_attr_name = (char *) sub_tuple_set.schema().fields()[0].field_name();
        bool flag = false;
        for (size_t j = 0; j < sub_tuples.size(); j++) {
          Tuple &tmp_sub_tuple = sub_tuples[j];
          int sub_index = sub_tuple_set.get_schema().index_of_field(sub_table_name, sub_attr_name);
          if (sub_index == -1)  sub_index = 0;
          V &sub_curr = dynamic_cast<V &>(tmp_sub_tuple.get_mutable(sub_index));
          if (curr.compare(sub_curr) == 0) {
            flag = true;
            break;
          }
        }
        if (sub_tuple_set.size() == 0 || flag == false)     // ! 空表或false
          final_tuple_set.add(std::move(tuples[i]));
      } break;
      default: 
        return RC::INVALID_ARGUMENT;
    }
  }
  return RC::SUCCESS;
}

// * 按照条件，最终合并父查询与子查询的集合
template<typename V>
RC ExecuteStage::merge_for_sub_query(TupleSet &sub_tuple_set,
                                     TupleSet &fa_tuple_set,
                                     const Selects &selects,
                                     const Condition condition,
                                     const AttrType attr_type,
                                     const AggrType aggr_type) {
  TupleSet final_tuple_set;
  final_tuple_set.set_schema(fa_tuple_set.schema());
  RC rc = RC::SUCCESS;
  rc = calculate_for_sub_query<V>(fa_tuple_set, final_tuple_set, sub_tuple_set, condition, selects);
  if (rc != RC::SUCCESS)
    return rc;
  memset(&fa_tuple_set, 0, sizeof(TupleSet));
  if (aggr_type != UNDEFINEDAGGR) {
    TupleValue *result;
    execute_for_aggr(attr_type, aggr_type, final_tuple_set, 0, result);
    Tuple tmp_tuple;
    if (result != nullptr) {
      tmp_tuple.add(result);
      fa_tuple_set.add(std::move(tmp_tuple));
    }
    fa_tuple_set.set_schema(final_tuple_set.schema());
  }
  else {
    for (size_t i = 0; i < final_tuple_set.mutable_tuples().size(); i++)  {
      Tuple tmp_tuple;
      for (const std::shared_ptr<TupleValue> value : final_tuple_set.get(i).values()) {
        tmp_tuple.add(value);
      }
      fa_tuple_set.add(std::move(tmp_tuple));
    }
    fa_tuple_set.set_schema(final_tuple_set.schema());
  }
  return RC::SUCCESS;
}

RC get_index_and_flag(int &index, int &flag, const Selects selects, std::vector<Condition> &conditions_list) {
  for (size_t i = index; i < selects.condition_num; i++) {
    auto right_attr_ =  selects.conditions[i].right_attr,  left_attr_ =  selects.conditions[i].left_attr;  
    auto right_value_ = selects.conditions[i].right_value, left_value_ = selects.conditions[i].left_value;
    bool condition_1 = right_attr_.attribute_name == nullptr &&
      right_attr_.relation_name == nullptr && right_value_.data == nullptr;
    bool condition_2 = left_attr_.attribute_name == nullptr &&
      left_attr_.relation_name == nullptr && left_value_.data == nullptr;
    bool condition_3 = condition_1 && condition_2;
    if (condition_3) {   // 如果条件里有嵌套子查询的情况
      conditions_list.emplace_back(selects.conditions[i]);
      index = i;
      flag = 2;
      break;
    }
    if (condition_1) {   // 如果条件里有嵌套子查询的情况
      conditions_list.emplace_back(selects.conditions[i]);
      index = i;
      flag = 0;
      break;
    }
    if (condition_2) {   // 如果条件里有嵌套子查询的情况
      conditions_list.emplace_back(selects.conditions[i]);
      index = i;
      flag = 1;
      break;
    }

  }
  return RC::SUCCESS;
}
bool calculate(double v1, double v2, CompOp comp);
bool calculate(double v1, double v2, CompOp comp) {
  switch (comp) {
    case (EQUAL_TO) : {
      return v1 == v2;
    } break;
    case (LESS_EQUAL) : {
      return v1 <= v2;
    } break;
    case (NOT_EQUAL) : {
      return v1 != v2;
    } break;
    case (LESS_THAN) : {
      return v1 < v2;
    } break;
    case (GREAT_EQUAL) : {
      return v1 >= v2;
    } break;
    case (GREAT_THAN) : {
      return v1 > v2;
    } break;
    case (IN): {
        return v1 == v2;
    } break;
    case (NOT_IN): {
        return v1 != v2;
    } break;
    default: {
      return false;
    } break;
  }
  return false;
}

RC ExecuteStage::merge_for_two_sub_query(const Selects &selects, TupleSet &sub1, TupleSet &sub2, Condition condition, TupleSet &new_tuple_set) {
  std::vector<Tuple> &tuples1 = sub1.mutable_tuples();
  std::vector<Tuple> &tuples2 = sub2.mutable_tuples();
  double v1 = ((NumericalValue &)tuples1[0].get(0)).get_value();
  double v2 = ((NumericalValue &)tuples2[0].get(0)).get_value();
  if (!calculate(v1,v2, condition.comp)) {
    TupleSet tmp_tuple_set;
    tmp_tuple_set.set_schema(new_tuple_set.get_schema());
    new_tuple_set.clear();
    new_tuple_set.set_schema(tmp_tuple_set.get_schema());
  }
  return RC::SUCCESS;
}


// * 执行得到父查询的元组集
RC ExecuteStage::execute_for_fa_query(std::vector<TupleSet> &sub_tuple_set, Selects &selects, TupleSet &new_tuple_set, const char *db, Session *session, Trx *trx, Query *sql) {
  Selects tmp_selects = selects;
  RC rc = RC::SUCCESS;
  std::vector<Condition> conditions_list;
  int index = 0;
  int flag = 1;    // * 0: 子查询在右， 1: 子查询在左  2:两边都有
  AggrType aggr_type = UNDEFINEDAGGR;
  if (selects.attributes[0].aggr_type == UNDEFINEDAGGR)
    rc = do_single_select(db, tmp_selects, new_tuple_set, session, trx);
  else {
    aggr_type = tmp_selects.attributes[0].aggr_type;
    tmp_selects.attributes[0].aggr_type = UNDEFINEDAGGR;
    rc = do_single_select(db, tmp_selects, new_tuple_set, session, trx);
  }
  for (size_t i = 0; i < sub_tuple_set.size(); i++) {
    get_index_and_flag(index, flag, selects, conditions_list);
    if (flag == 2) {
      TupleSet sub1, sub2;
      rc = do_single_select(db, sql->sstr.selection[1], sub1, session, trx);
      rc = do_single_select(db, sql->sstr.selection[2], sub2, session, trx);
      rc = merge_for_two_sub_query(selects, sub1, sub2, conditions_list[0], new_tuple_set);
      return rc;
    }
    else {
      RelAttr attr = flag == 0 ? selects.conditions[index].left_attr : selects.conditions[index].right_attr;       // ! 获取到出现子查询的那个条件，默认只有一条
      char* field_name = attr.attribute_name;
      char* table_name = ( nullptr == attr.relation_name) ? selects.relations[0] : attr.relation_name ;                                                                                  // ! 进行元组遍历处理
      const FieldMeta* field_meta = DefaultHandler::get_default().find_table(db, table_name)->table_meta().field(field_name);  
      const AttrType field_type = field_meta->type();
      AttrType attr_type;
      if (selects.attr_num > 1 || 0 == strcmp(selects.attributes[0].attribute_name, "*"))
        attr_type = UNDEFINED;
      else 
        attr_type = DefaultHandler::get_default().find_table(db, table_name)->table_meta().field(selects.attributes[0].attribute_name)->type();
      TupleSet sub_tuple_set_ = std::move(sub_tuple_set[i]);
      switch (field_type) {
        case INTS: {
          rc = merge_for_sub_query<IntValue>(sub_tuple_set_, new_tuple_set, selects, conditions_list[index], attr_type, aggr_type);
        } break;
        case FLOATS: {
          rc = merge_for_sub_query<FloatValue>(sub_tuple_set_, new_tuple_set, selects, conditions_list[index], attr_type, aggr_type);
        } break;
        case DATES:
        case CHARS: {
          rc = merge_for_sub_query<StringValue>(sub_tuple_set_, new_tuple_set, selects, conditions_list[index], attr_type, aggr_type);
        } break;
        default: {
          break;
        }
      }
      index++;
    } 
  }
  return rc;
}

RC ExecuteStage::check_for_sub_query(const Selects &selects,
                                     const Selects &sub_selects) {
  if (0 == strcmp("*", sub_selects.attributes->attribute_name) || sub_selects.attr_num > 1)
    return RC::ABORT;
  for (size_t i = 0; i < selects.condition_num; i++) {
    if (selects.conditions[i].right_attr.attribute_name == nullptr &&
        selects.conditions[i].right_attr.relation_name == nullptr &&
        selects.conditions[i].right_value.data == nullptr) {
      if (sub_selects.attributes[0].aggr_type == UNDEFINEDAGGR &&
          selects.conditions[i].comp != NOT_IN &&
          strcmp(sub_selects.attributes->attribute_name,
                 selects.conditions[i].left_attr.attribute_name) != 0)
        return RC::ABORT;
    } else if (selects.conditions[i].left_attr.attribute_name == nullptr &&
               selects.conditions[i].left_attr.relation_name == nullptr &&
               selects.conditions[i].left_value.data == nullptr) {
      if (sub_selects.attributes[0].aggr_type == UNDEFINEDAGGR &&
          selects.conditions[i].comp != NOT_IN &&
          strcmp(sub_selects.attributes->attribute_name,
                 selects.conditions[i].right_attr.attribute_name) != 0)
        return RC::ABORT;
    }
  }
  return RC::SUCCESS;
}





RC ExecuteStage::insert_for_map(int &index, TupleSchema &tuple_schema, const char *table_name, const char *attr_name, std::map<std::string, int> &name2pos, std::string expr_name, std::vector<int> &pos) {
  for (size_t i = 0; i < tuple_schema.fields().size(); i++) {
    auto tmp_field = tuple_schema.fields()[i];
    if ((table_name == nullptr || strcmp(table_name, tmp_field.table_name()) == 0)
    && attr_name != nullptr && tmp_field.field_name() != nullptr && strcmp(attr_name, tmp_field.field_name()) == 0) {
      index = i;
      name2pos.insert(std::make_pair(expr_name, index));
      break;
    }
  }
  return RC::SUCCESS;
}

RC ExecuteStage::set_expr_var(Expr &expr, std::map<std::string, int> &name2pos, TupleSchema tuple_schema, const Tuple &tmp_tuple, std::vector<int> &pos, const Selects selects) {
  int index = 0;
  RC rc = RC::SUCCESS;
  const char *table_name = nullptr, *attr_name = nullptr;
  for (auto expr_name : expr.getExpr()) {
    index = 0;
    table_name = expr_name.second[0]->table_name;
    attr_name = expr_name.second[0]->attribute_name;
    if (name2pos.find(expr_name.first) != name2pos.end()) 
      index = name2pos[expr_name.first];
    else
      rc = insert_for_map(index, tuple_schema, table_name, attr_name, name2pos, expr_name.first, pos);
    double tmp_value = ((NumericalValue&) (tmp_tuple.get(index))).get_value();
    expr.set_var(table_name, attr_name, tmp_value);
  }
  return RC::SUCCESS;
}

// * 计算condition_list具体的逻辑，更新new_tuple_set
RC ExecuteStage::calculate_for_expr(TupleSet &new_tuple_set, Selects selects, std::vector<Condition> condition_list, std::vector<int> &pos, TupleSchema &tuple_schema) {
  std::vector<Tuple> &tuples = new_tuple_set.mutable_tuples();
  TupleSet final_tuple_set;
  final_tuple_set.set_schema(new_tuple_set.schema());
  std::map<std::string, int> name2pos;
  RC rc = RC::SUCCESS;
  for (size_t k = 0; k < tuples.size(); k++) {
    Tuple tmp_tuple;
    for (size_t j = 0; j < selects.attr_num; j++) {
      RelAttr attr = selects.attributes[j];
      bool has_expr = false;
      TupleValue *result;
      if (attr.attribute_name == nullptr && attr.relation_name == nullptr && attr.expr != nullptr) {
        Expr *expr = new Expr;
        rc = expr->init(attr.expr);
        if (rc != RC::SUCCESS)  return rc;
        rc = set_expr_var(*expr, name2pos, tuple_schema, tuples[k], pos, selects);
        if (rc != RC::SUCCESS)  return rc;
        result = expr->evaluate();  
        has_expr = true;
      }
      for (auto condition: condition_list) {    // ! 暂定其实condition_list只有一条
        if (!condition.left_is_attr && !condition.right_is_attr && condition.left_value.type == ARITHMETIC_EXPR) {
          Expr left_expr;
          left_expr.init(condition.left_value.data);
          Expr right_expr;
          right_expr.init(condition.right_value.data);
          rc = set_expr_var(left_expr, name2pos, tuple_schema, tuples[k], pos, selects);
          if (rc != RC::SUCCESS)  return rc;
          rc = set_expr_var(right_expr, name2pos, tuple_schema, tuples[k], pos, selects);
          if (rc != RC::SUCCESS)  return rc;
          std::auto_ptr<TupleValue> left_val(left_expr.evaluate());
          std::auto_ptr<TupleValue> right_val(right_expr.evaluate());
          int res = 0;
          if (left_val->is_null() || right_val->is_null()) {
            res = 0;
          } else {
            switch (condition.comp) {
              case (EQUAL_TO): {
                res = left_val->compare(*right_val) == 0 ? 1 : 0;
              } break;
              case (LESS_EQUAL): {
                res = left_val->compare(*right_val) <= 0 ? 1 : 0;
              } break;
              case (GREAT_EQUAL): {
                res = left_val->compare(*right_val) >= 0 ? 1 : 0;
              } break;
              case (GREAT_THAN): {
                res = left_val->compare(*right_val) > 0 ? 1 : 0;
              } break;
              case (LESS_THAN): {
                res = left_val->compare(*right_val) < 0 ? 1 : 0;
              } break;
              default : break;
            }
          }
          if (res) {
            if (has_expr)
              tmp_tuple.add(result);
            else {
              char *attr_name = attr.attribute_name;
              char *table_name = attr.relation_name  == nullptr ? selects.relations[0] : attr.relation_name;
              if (attr_name != nullptr && strcmp(attr_name, "*") != 0) {
                int index = tuple_schema.index_of_field(table_name, attr_name);
                tmp_tuple.add(tuples[k].values()[index]);
              }
              else {
                for (int i: pos)
                  tmp_tuple.add(tuples[k].get_pointer(i));
              }
            }
          }
        }
      }
    }
    if (tmp_tuple.size() != 0)
      final_tuple_set.add(std::move(tmp_tuple));
  }
  // * 将值传回new_tuple_set
  memset(&new_tuple_set, 0, sizeof(TupleSet));
  for (size_t i = 0; i < final_tuple_set.mutable_tuples().size(); i++)  {
    Tuple tmp_tuple;
    for (const std::shared_ptr<TupleValue> value : final_tuple_set.get(i).values()) {
      tmp_tuple.add(value);
    }
    new_tuple_set.add(std::move(tmp_tuple));
  }
  new_tuple_set.set_schema(final_tuple_set.schema());
  return RC::SUCCESS;
}

RC ExecuteStage::execute_for_expr(const char *db, Selects &selects, TupleSet &new_tuple_set, Session *session, Trx *trx) {
    std::vector<Condition> condition_list;
    std::vector<int> *pos = new std::vector<int>();
    TupleSchema *tuple_schema = new TupleSchema();
    for (size_t i = 0; i < selects.condition_num; i++) {
      Condition condition = selects.conditions[i];
      if (!condition.left_is_attr && !condition.right_is_attr && condition.left_value.type == ARITHMETIC_EXPR) {
        condition_list.push_back(condition);
      }
    }
    RC rc = RC::SUCCESS;
    rc = do_single_select(db, selects, new_tuple_set, session, trx, true, pos, tuple_schema);
    if (rc != RC::SUCCESS)
      return rc;
    rc = calculate_for_expr(new_tuple_set, selects, condition_list, *pos, *tuple_schema);
    if (rc != RC::SUCCESS)
      return rc;
    return RC::SUCCESS;
}
bool check_condition_is_subquery(Condition condition);
bool check_if_has_sub_query(const Selects selects);
bool check_condition_is_subquery(Condition condition) {
  bool condition_1 = condition.right_is_attr && !condition.left_is_attr && condition.left_attr.attribute_name == nullptr && condition.left_attr.relation_name == nullptr && condition.left_value.data == nullptr;
  bool condition_2 = condition.left_is_attr && !condition.right_is_attr && condition.right_attr.attribute_name == nullptr && condition.right_attr.relation_name == nullptr && condition.right_value.data == nullptr;
  bool condition_3 = !condition.right_is_attr && !condition.left_is_attr && condition.left_attr.attribute_name == nullptr && condition.left_attr.relation_name == nullptr && condition.left_value.data == nullptr
  && condition.right_attr.attribute_name == nullptr && condition.right_attr.relation_name == nullptr && condition.right_value.data == nullptr;
  if (condition_1 || condition_2 || condition_3)
    return true;
  return false;
}

bool check_if_has_sub_query(const Selects selects) {
  for (size_t i = 0; i < selects.condition_num; i++) {
    Condition condition = selects.conditions[i];
    if (check_condition_is_subquery(condition)) {
      return true;
    }
  }
  return false;
}

RC ExecuteStage::do_sub_query(Selects* selects, Session *session, Trx *trx, const char *db, 
  TupleSet &new_tuple_set, Query *sql, Selects *parent_selects) {
  RC rc = RC::SUCCESS;
  if (check_if_has_sub_query(*selects)) {
    std::vector<TupleSet> sub_tuple_sets;
    Selects *ori_selects = selects;
    for (size_t i = 0; i < ori_selects->condition_num; i++) {
      Condition condition = ori_selects->conditions[i];
      TupleSet sub_tuple_set;
      if (check_condition_is_subquery(condition)) {
        selects++;
        rc = check_for_sub_query(*ori_selects, *selects);
        if (rc != RC::SUCCESS)  return rc;
        do_sub_query(selects, session, trx, db, sub_tuple_set, sql, ori_selects);
        sub_tuple_sets.emplace_back(std::move(sub_tuple_set));
      }
    }
    rc = execute_for_fa_query(sub_tuple_sets, *ori_selects, new_tuple_set, db, session, trx, sql);
    if (rc != RC::SUCCESS)  return rc; 
  } 
  else {
    rc = do_single_select(db, *selects, new_tuple_set, session, trx, false, nullptr, nullptr, parent_selects);
    if (rc != RC::SUCCESS)  return rc;
  }
  return rc;
}

RC ExecuteStage::do_select(const char *db, Query *sql, SessionEvent *session_event, size_t select_num) {
  RC rc = RC::SUCCESS;
  Session *session = session_event->get_client()->session;
  Trx *trx = session->current_trx();
  Selects &selects = sql->sstr.selection[0];
  TupleSet new_tuple_set;
  if (select_num > 1) {
    rc = do_sub_query(&selects, session, trx, db, new_tuple_set, sql, nullptr);
  }
  else {  // ? 如果没有子查询
    bool has_expr = false;
    for (size_t i = 0; i < selects.condition_num; i++) {
      Condition condition = selects.conditions[i];
      if (!condition.left_is_attr && !condition.right_is_attr && condition.left_value.type == ARITHMETIC_EXPR) {
        has_expr = true;
        break;
      }
    }
    if (has_expr)     // ? 如果有表达式
      rc = execute_for_expr(db, selects, new_tuple_set, session, trx);
    else 
      rc = do_single_select(db, selects, new_tuple_set, session, trx);
    if (rc != RC::SUCCESS)
      return rc;
  }
  std::stringstream ss;
  new_tuple_set.print(ss, selects.relation_num > 1);
  session_event->set_response(ss.str());
  end_trx_if_need(session, trx, true);
  return rc;
}

RC check_select_sql(Selects & selects, const char * db) {
  std::unordered_set<std::string> table_set;
  for (size_t i = 0; i < selects.relation_num; i++) {
    table_set.insert(std::string(selects.relations[i]));
  }
  // * 检查条件
  for (size_t i = 0; i < selects.condition_num ;++i) {
    Condition condition = selects.conditions[i];
    if (condition.left_is_attr) {
      if (condition.left_attr.relation_name != nullptr && table_set.count(std::string(condition.left_attr.relation_name)) == 0 ){
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
    }
    if (condition.right_is_attr) {
      if (condition.right_attr.relation_name != nullptr && table_set.count(std::string(condition.right_attr.relation_name)) == 0 ){
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
    }
    if (!condition.left_is_attr && !condition.right_is_attr && condition.left_value.type == ARITHMETIC_EXPR) {
      Expr left_expr;
      left_expr.init(condition.left_value.data);
      Expr right_expr;
      right_expr.init(condition.right_value.data);
      for (auto tmp_name: left_expr.getExpr()) {
        const char *table_name = tmp_name.second[0]->table_name;
        const char *attr_name = tmp_name.second[0]->attribute_name;
        if (table_name != nullptr && table_set.count(std::string(table_name)) == 0 ){
          return RC::SCHEMA_TABLE_NOT_EXIST;
        }
      }
      for (auto tmp_name: right_expr.getExpr()) {
        const char *table_name = tmp_name.second[0]->table_name;
        const char *attr_name = tmp_name.second[0]->attribute_name;
        if (table_name != nullptr && table_set.count(std::string(table_name)) == 0 ){
          return RC::SCHEMA_TABLE_NOT_EXIST;
        }
      }
    }
  }

  // * 检查排序
  for (size_t i = 0;i < selects.order_num; ++i) {
    RelAttr attr = selects.orders[i].attribute;
    const FieldMeta * field;
    const Table * table;
    if (table_set.size() == 1) {
      table = DefaultHandler::get_default().find_table(db, selects.relations[0]);
    }
    else {
      table = DefaultHandler::get_default().find_table(db, attr.relation_name);
    }
    if (nullptr == table) {
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    field = table->table_meta().field(attr.attribute_name);
    if (nullptr == field) {
        return RC::SCHEMA_FIELD_NOT_EXIST;
    }
  }

  // * 检查分组
  for (size_t i = 0; i < selects.group_num; ++i) {
    const RelAttr& attr = selects.groups[i].attribute;
    const FieldMeta * field;
    const Table * table;
    if (table_set.size() == 1) {
      table = DefaultHandler::get_default().find_table(db, selects.relations[0]);
    }
    else {
      table = DefaultHandler::get_default().find_table(db, attr.relation_name);
    }
    if (nullptr == table) {
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    field = table->table_meta().field(attr.attribute_name);
    if (nullptr == field) {
        return RC::SCHEMA_FIELD_NOT_EXIST;
    }
  }

  // * 检查属性
  for (int i = 0; i < selects.attr_num; i++) {
    const RelAttr& attr = selects.attributes[i]; 
    if (nullptr != attr.relation_name &&
        table_set.find(std::string(attr.relation_name)) == table_set.end()) {
      LOG_ERROR("ExecuteStage::do_select table not found %s", attr.relation_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    if (nullptr == attr.relation_name && selects.relation_num > 1 && attr.aggr_type != COUNT && attr.attribute_name != nullptr && 0 != strcmp(attr.attribute_name,"*")) {
      return RC::SQL_SYNTAX;  // 多表又没有写表名，除了count和*不然都是不能判断的
    }
    if (nullptr == attr.relation_name && nullptr == attr.attribute_name && attr.expr != nullptr) {
      Expr *expr = new Expr;
      RC rc = expr->init(attr.expr);
      if (rc != RC::SUCCESS)  return rc;
      for (auto tmp_name: expr->getExpr()) {
        const char *table_name = tmp_name.second[0]->table_name;
        const char *attr_name = tmp_name.second[0]->attribute_name;
        if (table_name != nullptr && table_set.find(std::string(table_name)) == table_set.end()){
          return RC::SCHEMA_TABLE_NOT_EXIST;
        }
      }
    }
  }

  return RC::SUCCESS;
}

RC ExecuteStage::group_aggr(std::vector<TupleSet> &group_tuple_sets,const std::vector<RelAttr> &attrs, 
      const std::vector<int> &tuple_pos, TupleSet &new_tuple_set,const Selects & selects,const char* db)  {
  RC rc = RC::SUCCESS;
  for (TupleSet &tmp_set: group_tuple_sets) {
    TupleSchema schema;  
    Tuple tmp_tuple;
    AttrType attr_type;
    const char * relation_name;
    const char * attr_name;
    std::string tmp_str;

    for (size_t i = 0; i < attrs.size(); ++i) {
      RelAttr attr = attrs[i];
      if (attr.aggr_type == UNDEFINEDAGGR) { // if not aggr, get the first value in this set
        if (tmp_set.size() > 0){
          tmp_tuple.add(tmp_set.get(0).get_pointer(tuple_pos[i]));
        }
        attr_type = tmp_set.get_schema().field(tuple_pos[i]).type();
        relation_name = tmp_set.get_schema().field(tuple_pos[i]).table_name();
        attr_name = tmp_set.get_schema().field(tuple_pos[i]).field_name();
      }
      else{
        TupleValue *result;
        AggrType aggr_type = attr.aggr_type;
        char* field_name = attr.attribute_name;
        
        prepare_print_for_schema(tmp_str, attr);
        attr_name = tmp_str.c_str();
        relation_name = "";

        if (aggr_type != COUNT && field_name != nullptr && 0 == strcmp("*", field_name))              // ! 如果聚合算子的类型不是count且属性名为*，返回失败
          return RC::SCHEMA_FIELD_MISSING;
        if (aggr_type == COUNT) {
          result = new IntValue(count_not_null_tuple(tmp_set.tuples(), tuple_pos[i]));
          attr_type = INTS;
        }
        else if (field_name == nullptr && attr.aggr_value != -1) {                 // ! 如果聚合算子的类型不是Count且属性名不为空且值是普通数字
          result = new FloatValue(attr.aggr_value);
          attr_type = FLOATS;
        }
        else {    
          char* table_name = ( nullptr == attr.relation_name) ? selects.relations[0] : attr.relation_name ;                                                                                  // ! 进行元组遍历处理
          const FieldMeta* field_meta = DefaultHandler::get_default().find_table(db, table_name)->table_meta().field(field_name);  // ! 单表所以直接取第一个元素
          const AttrType field_type = field_meta->type();                                              // ! 获取当前的属性值，int/float
          attr_type = field_type;
          rc = execute_for_aggr(field_type, aggr_type, tmp_set, tuple_pos[i], result);
        }

        tmp_tuple.add(result);
      }

      schema.add(attr_type , relation_name, attr_name);
    }
    if (tmp_set.size() > 0)
      new_tuple_set.add(std::move(tmp_tuple));
    new_tuple_set.set_schema(schema);
  }
  return rc;
}

RC ExecuteStage::merge_tuple_sets(std::vector<TupleSet> &group_tuple_sets, TupleSet &new_tuple_set) {
  for(size_t i = 0; i < group_tuple_sets.size(); i++) {
    Tuple final_tuple;
    if (group_tuple_sets[i].tuples().size() == 0)
      return RC::ABORT;
    for (auto value: group_tuple_sets[i].tuples()[0].values())
      final_tuple.add(value);
    new_tuple_set.add(std::move(final_tuple));
  }
  return RC::SUCCESS;
}

RC divide(std::vector<TupleSet> &group_tuple_sets, OrderBySort group_by, TupleSet &final_set) {
  size_t m = 0, n = 0;
  for (n = m + 1; n < final_set.mutable_tuples().size(); n++) {
    if (!group_by.compare(final_set.mutable_tuples()[m], final_set.mutable_tuples()[n])) {        // * 如果不相等就分割元组
      TupleSet tmp_set;
      while (m < n) {
        Tuple tmp_tuple;
        for (const std::shared_ptr<TupleValue> value : final_set.get(m).values()) {
          tmp_tuple.add(value);
        }
        tmp_set.add(std::move(tmp_tuple));
        m++;
      }
      tmp_set.set_schema(final_set.get_schema());
      group_tuple_sets.push_back(std::move(tmp_set));
    }
  }
  TupleSet tmp_set;
  for (size_t i = m; i < final_set.mutable_tuples().size(); i++){
    Tuple tmp_tuple;
    for (const std::shared_ptr<TupleValue> value : final_set.get(i).values()) {
      tmp_tuple.add(value);
    }
    tmp_set.add(std::move(tmp_tuple));
  }
  tmp_set.set_schema(final_set.get_schema());
  group_tuple_sets.push_back(std::move(tmp_set));
  return RC::SUCCESS;
}

RC ExecuteStage::execute_for_aggr(const AttrType field_type, const AggrType aggr_type,TupleSet &final_set, int index, TupleValue* &result) {
  switch (field_type) {
    case INTS: {
      result = scan_and_execute<IntValue>(field_type, aggr_type, final_set, index);
    } break;
    case FLOATS: {
      result = scan_and_execute<FloatValue>(field_type, aggr_type, final_set, index);
    } break;
    case DATES:
    case CHARS: {
      result = scan_and_execute<StringValue>(field_type, aggr_type, final_set, index);
    } break;
    default: { break; }
  }
  return RC::SUCCESS;
}

RC prepare_print_for_schema(std::string &aggr_name, RelAttr attr) {
  if (attr.aggr_type == UNDEFINEDAGGR)  {
    if (nullptr == attr.relation_name && nullptr == attr.attribute_name)
      return RC::SCHEMA_FIELD_NOT_EXIST;
    else if (nullptr == attr.relation_name && nullptr != attr.attribute_name)
      aggr_name = attr.attribute_name;
    else if (nullptr != attr.relation_name && nullptr != attr.attribute_name && -1 == attr.aggr_value)
      aggr_name = std::string(attr.relation_name) + "." + std::string(attr.attribute_name);
    else  
      return RC::ABORT;
    return RC::SUCCESS;
  }
  else {
    if (nullptr == attr.relation_name && nullptr == attr.attribute_name) {
      std::ostringstream oss;
      oss << attr.aggr_value;
      aggr_name = aggr_type_to_string[attr.aggr_type] + "(" + oss.str() +  ")";
    }
    else if (nullptr == attr.relation_name && nullptr != attr.attribute_name)
      aggr_name = aggr_type_to_string[attr.aggr_type] + "(" + attr.attribute_name +  ")";
    else if (nullptr != attr.relation_name && nullptr != attr.attribute_name)
      aggr_name = aggr_type_to_string[attr.aggr_type] + "(" + attr.relation_name + "." + attr.attribute_name +  ")";
    else  
      return RC::ABORT;
    return RC::SUCCESS;
  }
}

template<typename V>
TupleValue* ExecuteStage::scan_and_execute(const AttrType field_type, const AggrType aggr_type, TupleSet &tuple_set, int index) {
  TupleValue* result_value = nullptr;
  if (tuple_set.size() == 0) return result_value;
  switch (aggr_type) {
    case MIN:
    case MAX: {
      std::vector<Tuple> &tuples = tuple_set.mutable_tuples();
      V* tmp_value = nullptr;
      for (size_t i = 0; i < tuples.size(); ++i) {
        Tuple & tmp_tuple = tuples[i];
        V& curr = dynamic_cast<V &>(tmp_tuple.get_mutable(index));
        
        if (tmp_value == nullptr) {
          tmp_value = new V(curr.get_value(), curr.is_null());
        } else if (!curr.is_null() && (tmp_value->is_null() || (aggr_type == MAX && curr.compare(*tmp_value) > 0) || (aggr_type == MIN && curr.compare(*tmp_value) < 0))) {
          tmp_value = new V(curr.get_value(), curr.is_null());
        }
      }
      result_value = tmp_value;
    } 
    break;
    case AVG: {
      float tmp_result;
      bool not_null = false;
      if (field_type == INTS) {
        IntValue *tmp_value = new IntValue(0);
        for (Tuple &tmp_tuple : tuple_set.mutable_tuples()) {
          IntValue &curr = (IntValue &)(tmp_tuple.get_mutable(index));
          if (curr.is_null()) {
            continue;
          }
          tmp_value->add(curr);
          not_null = true;
        }
        tmp_result = (float)tmp_value->get_value();
        delete tmp_value;
        tmp_value = nullptr;
      } else if (field_type == FLOATS) {
        FloatValue *tmp_value = new FloatValue(0.0);
        for (Tuple &tmp_tuple : tuple_set.mutable_tuples()) {
          FloatValue &curr = (FloatValue &)(tmp_tuple.get_mutable(index));
          if (curr.is_null()) {
            continue;
          }
          tmp_value->add(curr);
          not_null = true;
        }
        tmp_result = tmp_value->get_value();
        delete tmp_value;
        tmp_value = nullptr;
      }
      float avg_result = tmp_result / count_not_null_tuple(tuple_set.tuples(), index);
      result_value = new FloatValue(avg_result, !not_null && tuple_set.tuples().size() > 0);  // ! 统一浮点数存储
    } break;
    case COUNT: {
      result_value = new IntValue(count_not_null_tuple(tuple_set.tuples(), index));
    } break;
    default: {
      LOG_ERROR("Not a valid aggragate op");
      break;
    }
  }
  return result_value;
}

int count_not_null_tuple(const std::vector<Tuple> &tuples, int index){
  int cnt = 0;
  if (index == -1){
    return tuples.size();
  }
  
  for(const Tuple &tuple: tuples){
    cnt += !tuple.get(index).is_null();
  }
  return cnt;
}

bool match_table(const Selects &selects, const char *table_name_in_condition,
                  const char *table_name_to_match) {
  if (table_name_in_condition != nullptr) {
    return 0 == strcmp(table_name_in_condition, table_name_to_match);
  }

  return selects.relation_num == 1;
}

static RC schema_add_field(Table * table, const char *field_name,
                            TupleSchema &schema) {
  const FieldMeta *field_meta = table->table_meta().field(field_name);
  if (nullptr == field_meta) {
    LOG_WARN("No such field. %s.%s", table->name(), field_name);
    return RC::SCHEMA_FIELD_MISSING;
  }

  schema.add_if_not_exists(field_meta->type(), table->name(),
                            field_meta->name());
  return RC::SUCCESS;
}

// 把所有的表和只跟这张表关联的condition都拿出来，生成最底层的select 执行节点
RC create_selection_executor(Trx *trx, Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node) {
  // 列出跟这张表关联的Attr
  TupleSchema schema;
  Table * table = DefaultHandler::get_default().find_table(db, table_name);
  if (nullptr == table) {
    LOG_WARN("No such table [%s] in db [%s]", table_name, db);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  for (int i = 0; i < selects.attr_num; i++) {
    RelAttr &attr = selects.attributes[i];
    if (attr.relation_name == NULL && attr.attribute_name == NULL && attr.expr != NULL) {       // ! 针对attr的expr
      Expr *expr = new Expr;
      RC rc = expr->init(attr.expr);
      std::string output = expr->to_string();
      if (rc != RC::SUCCESS) return rc;
      for (auto expr_attr: expr->getExpr()) {
        const char *attr_name = expr_attr.second[0]->attribute_name;
        const char *table_name_expr = expr_attr.second[0]->table_name == nullptr ? selects.relations[0] :expr_attr.second[0]->table_name;
        assert(table_name_expr != nullptr);
        if (table_name_expr != nullptr && 0 == strcmp(table_name_expr, table_name)) {
          AttrType type = table->table_meta().field(attr_name)->type();
          schema.add_if_not_exists(type, table_name, attr_name);
        }
      }
    }
    else {
      if (attr.aggr_type != UNDEFINEDAGGR && attr.aggr_value != -1) {   // ! 暂时的想法是让所有的聚合生成一个扫描完所有行+列的select算子
        TupleSchema::from_table(table, schema); break;
      }
      else if (nullptr == attr.relation_name || 0 == strcmp(table_name, attr.relation_name)) {
        if (0 == strcmp("*", attr.attribute_name)) {  // ! 列出这张表所有字段
          TupleSchema::from_table(table, schema);     // ! 没有校验，给出* 之后，再写字段的错误
          break; 
        } 
        else {
          RC rc = schema_add_field(table, attr.attribute_name, schema); // ! 列出这张表相关字段
          if (rc != RC::SUCCESS) {
            return rc;
          }
        }
      }
    }
  }

  // 找出仅与此表相关的过滤条件, 或者都是值的过滤条件
  std::vector<DefaultConditionFilter *> condition_filters;
  RC rc = RC::SUCCESS;
  for (size_t i = 0; i < selects.condition_num; i++) {
    const Condition &condition = selects.conditions[i];
    bool condition_1 = (condition.left_is_attr == 0 && condition.right_is_attr == 1 && match_table(selects, condition.right_attr.relation_name, table_name)),
         condition_2 = (condition.left_is_attr == 1 && condition.right_is_attr == 0 && match_table(selects, condition.left_attr.relation_name, table_name)),
         condition_3 = (condition.left_is_attr == 1 && condition.right_is_attr == 1 && match_table(selects, condition.left_attr.relation_name, table_name) 
                                                                                    && match_table(selects, condition.right_attr.relation_name, table_name)),
         condition_4 = (condition.left_is_attr == 0 && condition.right_is_attr == 0);
    if (condition_1 || condition_2 || condition_3 || condition_4) {
     if (condition_4  && condition.left_value.type == ARITHMETIC_EXPR) {      // ! 是表达式
        Expr left_expr;
        left_expr.init(condition.left_value.data);
        Expr right_expr;
        right_expr.init(condition.right_value.data);
        for (auto expr_attr: left_expr.getExpr()) {
          const char *attr_name = expr_attr.second[0]->attribute_name;
          const char *table_name_expr = expr_attr.second[0]->table_name == nullptr ? selects.relations[0] : expr_attr.second[0]->table_name;
          if (table_name_expr != nullptr && 0 == strcmp(table_name_expr, table_name)) {
            AttrType type = table->table_meta().field(attr_name)->type();
            schema.add_if_not_exists(type, table_name_expr, attr_name);
          }
        }
        for (auto expr_attr: right_expr.getExpr()) {
          const char *attr_name = expr_attr.second[0]->attribute_name;
          const char *table_name_expr = expr_attr.second[0]->table_name == nullptr ? selects.relations[0] : expr_attr.second[0]->table_name;
          if (table_name_expr != nullptr && 0 == strcmp(table_name_expr, table_name)) {
            AttrType type = table->table_meta().field(attr_name)->type();
            schema.add_if_not_exists(type, table_name_expr, attr_name);
          }
        }
      }
      // ! 包含子查询
      else if (condition_1 && condition.left_attr.attribute_name == nullptr && condition.left_attr.relation_name == nullptr && condition.left_value.data == nullptr) {
        const char *attr_name = condition.right_attr.attribute_name;
        const char *table_name = condition.right_attr.relation_name == nullptr ? selects.relations[0] : condition.right_attr.relation_name;
        AttrType type = table->table_meta().field(attr_name)->type();
        schema.add_if_not_exists(type, table_name, attr_name);
      }
      else if (condition_2 && condition.right_attr.attribute_name == nullptr && condition.right_attr.relation_name == nullptr && condition.right_value.data == nullptr) {
        const char *attr_name = condition.left_attr.attribute_name;
        const char *table_name = condition.left_attr.relation_name == nullptr ? selects.relations[0] : condition.left_attr.relation_name;
        AttrType type = table->table_meta().field(attr_name)->type();
        schema.add_if_not_exists(type, table_name, attr_name);
      }
      else if (condition_4 && condition.left_attr.attribute_name == nullptr && condition.left_attr.relation_name == nullptr && condition.left_value.data == nullptr
                           && condition.right_attr.attribute_name == nullptr && condition.right_attr.relation_name == nullptr && condition.right_value.data == nullptr) {
                             puts("both select");
      }

      else {        // ! 不是表达式也不是子查询就创建condition算子
        DefaultConditionFilter *condition_filter = new DefaultConditionFilter();
        condition_filters.push_back(condition_filter);
        rc = condition_filter->init(*table, condition);   // ! 初始化算子
        if (rc != RC::SUCCESS)
          break;
        if (condition_1) {  // 左边是值，右边是属性名
          const FieldMeta *field_right =
              table->table_meta().field(condition.right_attr.attribute_name);
          if (field_right->type() == AttrType::DATES && condition.right_value.type != NONE) {
            Date date;
            rc = date.readAndValidate(
                reinterpret_cast<char *>(condition.left_value.data));
            if (rc != RC::SUCCESS) {
              LOG_ERROR("Condition_1 failed");
              return rc;
            }
          }
        } 
        else if (condition_2) {  // 左边是属性名，右边是值
          const FieldMeta *field_left =
              table->table_meta().field(condition.left_attr.attribute_name);
          if (field_left->type() == AttrType::DATES && condition.right_value.type != NONE) {
            Date date;
            rc = date.readAndValidate(
                reinterpret_cast<char *>(condition.right_value.data));
            if (rc != RC::SUCCESS) {
              LOG_ERROR("Condition_2 failed");
              return rc;
            }
          }
        }
      }
    }
    else if (condition.left_is_attr == 1 && condition.right_is_attr == 1 && (match_table(selects, condition.left_attr.relation_name, table_name) 
                                                                         || match_table(selects, condition.right_attr.relation_name, table_name))){
        RelAttr attr;
        Table * left_table = DefaultHandler::get_default().find_table(db, condition.left_attr.relation_name);
        Table * right_table = DefaultHandler::get_default().find_table(db, condition.right_attr.relation_name);

        if (left_table == nullptr || right_table == nullptr){
          return RC::SCHEMA_TABLE_NOT_EXIST;
        }
        const FieldMeta *field_left =
          left_table->table_meta().field(condition.left_attr.attribute_name);
        const FieldMeta *field_right =
            right_table->table_meta().field(condition.right_attr.attribute_name);
        if (field_left == nullptr || nullptr == field_right){
          return RC::SCHEMA_FIELD_NOT_EXIST;
        }
        
        if (field_left->type() != field_right->type() ) {
          LOG_ERROR("create_selection_executor: type mismatch");
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      if (match_table(selects, condition.left_attr.relation_name, table_name))
        attr = condition.left_attr;
      else
        attr = condition.right_attr;
      AttrType type = table->table_meta().field(attr.attribute_name)->type();
      schema.add_if_not_exists(type,  attr.relation_name, attr.attribute_name);   // ! 列出这张表相关字段,用于后续join，只用添加现在不存在的
    }
  }

  // ! 为聚合算子添加属性列
  for (int j = selects.group_num - 1; j >= 0; j--) {
    const RelAttr &attr = selects.groups[j].attribute;
    AttrType type;
    if (selects.relation_num == 1)
      type = table->table_meta().field(attr.attribute_name)->type();
    else if (selects.relation_num > 1) {
      if (strcmp(attr.relation_name, table_name) != 0) continue;
      else
        type = table->table_meta().field(attr.attribute_name)->type();
    }
    schema.add_if_not_exists(type, table_name, attr.attribute_name);
  }

  if (rc != RC::SUCCESS) {
    for (DefaultConditionFilter *&filter : condition_filters) {
      delete filter;
      filter = nullptr;
    }
    return rc;
  }

  return select_node.init(trx, table, std::move(schema), std::move(condition_filters));
}

TupleSchema gen_schema(const Selects &selects , const char * db){
  TupleSchema schema;
  for (int i = 0; i < selects.attr_num; i++) {
    RelAttr attr = selects.attributes[i];
    if  (attr.attribute_name != nullptr && 0 == strcmp("*", attr.attribute_name)) {  // 列出这张表所有字段
      if  (nullptr == attr.relation_name){// 加入全部表
        for (int i = selects.relation_num - 1; i >= 0; i--) {
          const char * table_name = selects.relations[i];
          Table * table = DefaultHandler::get_default().find_table(db, table_name);
          TupleSchema::from_table(table, schema);
        }
        break; // 没有校验，给出* 之后，再写字段的错误
      }
      else  {  // 加入对应表
        const char * table_name = attr.relation_name;
        Table * table = DefaultHandler::get_default().find_table(db, table_name);
        TupleSchema::from_table(table, schema);
      }
    }
    else if (attr.relation_name == nullptr && attr.attribute_name == nullptr && attr.expr != nullptr) {
      Expr *expr = new Expr;
      RC rc = expr->init(attr.expr);
      std::string expr_schema =  expr->to_string();
      char* table_name = "t_obcontest";   // ! 这里的表名不重要了
      schema.add_if_not_exists(FLOATS, table_name, expr_schema.c_str());
    }
    else {
      char* relation_name = attr.relation_name == nullptr ? selects.relations[0] : attr.relation_name;
      Table * table = DefaultHandler::get_default().find_table(db, relation_name);
      AttrType type = table->table_meta().field(attr.attribute_name)->type();
      schema.add_if_not_exists(type,table->name(),  attr.attribute_name);
    }
  }
  return schema;
}

// 将join_set合并到main_set中，并进行条件过滤
TupleSet merge_tuple_set(TupleSet &main_set,const TupleSet &join_set,std::vector<Condition> conditions){
  // 记录每个条件对应的tuple的位置和操作符
  std::vector<int> main_tuple_pos,join_tuple_pos;
  std::vector<CompOp> compops;
  for (Condition condition : conditions) {
    int main_pos = -1;
    CompOp compop = EQUAL_TO;
    for (size_t i = 0;i < main_set.schema().fields().size(); ++i) {
      TupleField field = main_set.schema().fields()[i];
      // 统一整理成main_set在左边，join_set在右边
      if (strcmp( field.table_name() , condition.left_attr.relation_name) == 0 && 
           strcmp( field.field_name() , condition.left_attr.attribute_name) ==0) {
        main_pos = i;
        compop = condition.comp;
        break;
      }
      else if (strcmp( field.table_name() , condition.right_attr.relation_name) == 0 && 
               strcmp( field.field_name() , condition.right_attr.attribute_name) ==0) {
        main_pos = i;
        if (condition.comp == CompOp::GREAT_EQUAL) {
            compop =  CompOp::LESS_EQUAL;
        } else if (condition.comp == CompOp::GREAT_THAN) {
            compop =  CompOp::LESS_THAN;
        } else if (condition.comp == CompOp::LESS_EQUAL) {
            compop =  CompOp::GREAT_EQUAL;
        } else if (condition.comp == CompOp::LESS_THAN) {
            compop =  CompOp::GREAT_THAN;
        } else {
            compop = condition.comp;
        }
        break;
        }    
    }

    int join_pos = -1;
    for (size_t i = 0; i < join_set.schema().fields().size(); ++i) {
      TupleField field = join_set.schema().fields()[i];
      if ((strcmp( field.table_name() , condition.left_attr.relation_name) == 0 && 
          strcmp( field.field_name() , condition.left_attr.attribute_name) ==0)
      || (strcmp( field.table_name() , condition.right_attr.relation_name) == 0 && 
          strcmp( field.field_name() , condition.right_attr.attribute_name) ==0)) {
        join_pos = i;
        break;
      }
    }
    join_tuple_pos.push_back(join_pos);   
    main_tuple_pos.push_back(main_pos);
    compops.push_back(compop);
  }

    // 生成新的schema
    TupleSchema schema;
    schema.append(main_set.schema());
    schema.append(join_set.schema());

    TupleSet new_tuple_set;
    new_tuple_set.set_schema(schema);

    for (const Tuple &main_tuple: main_set.tuples())
      for (const Tuple &join_tuple: join_set.tuples()) {
        bool satisfied = true;  // 判断是否满足condition
        for(size_t i = 0; i < conditions.size(); ++i) {
          if (main_tuple_pos[i] == -1 || join_tuple_pos[i] == -1) // 这个condition和自己无关
            continue;
          if ( main_tuple.get(main_tuple_pos[i]).is_null() || join_tuple.get(join_tuple_pos[i]).is_null()){
            satisfied = false;
            continue;
          }
          
          int cmp_result = main_tuple.get(main_tuple_pos[i]).compare(join_tuple.get(join_tuple_pos[i]));
          switch (compops[i]) {
          case EQUAL_TO:
            satisfied &= (0 == cmp_result);
            break;
          case LESS_EQUAL:
            satisfied &= (cmp_result <= 0);
            break;
          case NOT_EQUAL:
            satisfied &= (cmp_result != 0);
            break;
          case LESS_THAN:
            satisfied &= (cmp_result < 0);
            break;
          case GREAT_EQUAL:
            satisfied &= (cmp_result >= 0);
            break;
          case GREAT_THAN:
            satisfied &= (cmp_result > 0);
            break;
          default:
            break;
          }
        }
        // 如果满足条件，加入
        if (satisfied) {
          Tuple new_tuple;
          for (std::shared_ptr<TupleValue> value : main_tuple.values()) {
            new_tuple.add(value);
          }
          for (std::shared_ptr<TupleValue> value : join_tuple.values()) {
            new_tuple.add(value);
          }
          new_tuple_set.add(std::move(new_tuple));
        }
      }
    return  new_tuple_set;
}
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

#ifndef __OBSERVER_SQL_EXECUTE_STAGE_H__
#define __OBSERVER_SQL_EXECUTE_STAGE_H__

#include "common/seda/stage.h"
#include "sql/parser/parse.h"
#include "rc.h"
#include "storage/common/date.h"
#include "sql/executor/tuple.h"
#include "session/session.h"
#include "sql/executor/arithmetic_expr.h"
static std::map<AggrType, std::string> aggr_type_to_string= {
  {AggrType::MAX, "max"},
  {AggrType::MIN, "min"},
  {AggrType::AVG, "avg"},
  {AggrType::COUNT, "count"},
};

class SessionEvent;

class ExecuteStage : public common::Stage {
public:
  ~ExecuteStage();
  static Stage *make_stage(const std::string &tag);
  template<typename V>
  TupleValue* scan_and_execute(const AttrType field_type, AggrType aggr_type, TupleSet &tuple_set, int index);
  RC execute_for_aggr(const AttrType field_type, const AggrType aggr_type,TupleSet &final_set, int index, TupleValue* &result);
  RC execute_for_fa_query(std::vector<TupleSet> &sub_tuple_set, Selects &selects, TupleSet &new_tuple_set, const char *db, Session *session, Trx *trx, Query *sql);
  RC merge_tuple_sets(std::vector<TupleSet> &group_tuple_sets, TupleSet &new_tuple_set);
  RC merge_for_two_sub_query(const Selects &selects, TupleSet &sub1, TupleSet &sub2, Condition condition, TupleSet &new_tuple_set);
  template<typename V>
  RC merge_for_sub_query(TupleSet &sub_tuple_set, TupleSet &fa_tuple_set, const Selects &selects, const Condition condition, const AttrType attr_type, const AggrType aggr_type);
  template<typename V>
  RC calculate_for_sub_query(TupleSet &fa_tuple_set, TupleSet &final_tuple_set, TupleSet &sub_tuple_set, Condition condition, const Selects &selects);
  RC group_aggr(std::vector<TupleSet> &group_tuple_sets,const std::vector<RelAttr> &tuples_for_print, 
  const std::vector<int> &tuple_pos, TupleSet &new_tuple_set,const Selects & selects,const char* db);
  RC check_for_sub_query(const Selects &selects, const Selects &sub_selects);
  RC calculate_for_expr(TupleSet &new_tuple_set, Selects selects, std::vector<Condition> condition_list, std::vector<int> &pos, TupleSchema &tuple_schema);
  RC execute_for_expr(const char *db, Selects &selects, TupleSet &new_tuple_set, Session *session, Trx *trx);
  RC insert_for_map(int &index, TupleSchema &tuple_schema, const char *table_name, const char *attr_name, std::map<std::string, int> &name2pos, std::string expr_name, std::vector<int> &pos);
  RC set_expr_var(Expr &expr, std::map<std::string, int> &name2pos, TupleSchema tuple_schema, const Tuple &tmp_tuple, std::vector<int> &pos, const Selects selects);
  RC do_sub_query(Selects* selects, Session *session, Trx *trx, const char *db, TupleSet &new_tuple_set, Query *sql, Selects* parent_selects);
protected:
  // common function
  ExecuteStage(const char *tag);
  bool set_properties() override;

  bool initialize() override;
  void cleanup() override;
  void handle_event(common::StageEvent *event) override;
  void callback_event(common::StageEvent *event,
                     common::CallbackContext *context) override;

  void handle_request(common::StageEvent *event);
  RC do_select(const char *db, Query *sql, SessionEvent *session_event, size_t select_num);
  RC do_single_select(const char *db, Selects &selects, TupleSet &new_tuple_set, Session *session, Trx *trx, bool has_expr = false, std::vector<int>* pos = nullptr, TupleSchema *tuple_schema = nullptr, Selects *parent_selects=nullptr);
  TupleValue *init_for_numeric(const AttrType field_type, const AggrType aggr_type);
protected:
private:
  Stage *default_storage_stage_ = nullptr;
  Stage *mem_storage_stage_ = nullptr;
};

#endif //__OBSERVER_SQL_EXECUTE_STAGE_H__
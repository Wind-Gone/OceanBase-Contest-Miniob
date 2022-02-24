
%{

#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.tab.h"
#include "sql/parser/lex.yy.h"
// #include "common/log/log.h" // 包含C++中的头文件

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
typedef struct ParserContext {
  Query * ssql;
  size_t select_length;
  size_t from_length;
  size_t tuple_num;
  size_t aggr_length;
  InsertTuple tuples[MAX_NUM];
  Conditions condition_list[MAX_NUM];
  char id[MAX_NUM];
  AggrValue aggr_value[MAX_NUM];
  Values values[MAX_NUM];
  CompOps comps[MAX_NUM];
  int stack[MAX_NUM];
  int ptr;
} ParserContext;

//获取子串
char *substr(const char *s,int n1,int n2)/*从s中提取下标为n1~n2的字符组成一个新字符串，然后返回这个新串的首地址*/
{
  char *sp = malloc(sizeof(char) * (n2 - n1 + 2));
  int i, j = 0;
  for (i = n1; i <= n2; i++) {
    sp[j++] = s[i];
  }
  sp[j] = 0;
  return sp;
}

void yyerror(yyscan_t scanner, const char *str)
{
  ParserContext *context = (ParserContext *)(yyget_extra(scanner));
  query_reset(context->ssql);
  context->ssql->flag = SCF_ERROR;
  context->condition_list[0].condition_length = 0;
  context->condition_list[1].condition_length = 0;
  context->from_length = 0;
  context->select_length = 0;
  context->values[0].value_length = 0;
  context->values[1].value_length = 0;
  context->ssql->sstr.insertion.tuple_num = 0;
  context->aggr_length = 0;
  context->ssql->select_num = 0;
  context->ptr = 0;
  memset(context->stack,0,sizeof(int));
  printf("parse sql failed. error=%s", str);
}

ParserContext *get_context(yyscan_t scanner)
{
  return (ParserContext *)yyget_extra(scanner);
}

#define CONTEXT get_context(scanner)

%}

%define api.pure full
%lex-param { yyscan_t scanner }
%parse-param { void *scanner }

// 标识tokens
%token  SEMICOLON
        CREATE
        DROP
        TABLE
        TABLES
		UNIQUE
        INDEX
        SELECT
		INNER
		JOIN
		ORDER
		GROUP
		BY
        DESC
		ASC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE     
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
		TEXT_T
        STRING_T
        FLOAT_T
		DATE_T
		COUNT_T
		AVG_T
		MAX_T
		MIN_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        SET
        ON
        LOAD
        DATA
        INFILE
        EQ
        LT
        GT
        LE
        GE
        NE
		IN_T
		NOT_T
		IS
		V_NULL
		NULLABLE

%left 	PLUS_T
		MINUS_T

%left	STAR
		DIVIDE_T

%left   LBRACE
		RBRACE

%union {
  struct _Attr *attr;
  struct _RelAttr *other_aggr_attr1;
  struct _Condition *condition1;
  struct _Value *value1;
  struct _Parameter *aggr1;
  struct _Selects *select1;
  char *string;
  int number;
  float floats;
  char *position;
  void *expr1;
}

%token <number> POSITIVE_NUMBER
%token <floats> POSITIVE_FLOAT
%token <string> ID
%token <string> PATH
%token <string> SSS
%token <string> STAR
%token <string> STRING_V
//非终结符

%type <number> type;
%type <condition1> condition;
%type <value1> value;
%type <number> number;
%type <string> aggr_op;
%type <other_aggr_attr1> other_aggr_attr;
%type <expr1> expr;
%type <select1> select_sql;
%%

commands:		//commands or sqls. parser starts here.
    /* empty */
    | commands command
    ;

command:
	  select_  
	| insert
	| update
	| delete
	| create_table
	| drop_table
	| show_tables
	| desc_table
	| create_index	
	| drop_index
	| sync
	| begin
	| commit
	| rollback
	| load_data
	| help
	| exit
    ;

exit:			
    EXIT SEMICOLON {
        CONTEXT->ssql->flag=SCF_EXIT;//"exit";
    };

help:
    HELP SEMICOLON {
        CONTEXT->ssql->flag=SCF_HELP;//"help";
    };

sync:
    SYNC SEMICOLON {
      CONTEXT->ssql->flag = SCF_SYNC;
    }
    ;

begin:
    TRX_BEGIN SEMICOLON {
      CONTEXT->ssql->flag = SCF_BEGIN;
    }
    ;

commit:
    TRX_COMMIT SEMICOLON {
      CONTEXT->ssql->flag = SCF_COMMIT;
    }
    ;

rollback:
    TRX_ROLLBACK SEMICOLON {
      CONTEXT->ssql->flag = SCF_ROLLBACK;
    }
    ;

drop_table:		/*drop table 语句的语法解析树*/
    DROP TABLE ID SEMICOLON {
        CONTEXT->ssql->flag = SCF_DROP_TABLE;//"drop_table";
        drop_table_init(&CONTEXT->ssql->sstr.drop_table, $3);
    };

show_tables:
    SHOW TABLES SEMICOLON {
      CONTEXT->ssql->flag = SCF_SHOW_TABLES;
    }
    ;

desc_table:
    DESC ID SEMICOLON {
      CONTEXT->ssql->flag = SCF_DESC_TABLE;
      desc_table_init(&CONTEXT->ssql->sstr.desc_table, $2);
    }
    ;

create_index:		/*create index 语句的语法解析树*/
    CREATE INDEX ID ON ID LBRACE ID index_list RBRACE SEMICOLON 
		{
			CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";
			index_append_attribute(&CONTEXT->ssql->sstr.create_index, $7);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length ++;
			create_index_init(&CONTEXT->ssql->sstr.create_index, $3, $5);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		}
    |CREATE UNIQUE INDEX ID ON ID LBRACE ID index_list RBRACE SEMICOLON 
		{
			CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";
			index_append_attribute(&CONTEXT->ssql->sstr.create_index, $8);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length ++;
			create_unique_index_init(&CONTEXT->ssql->sstr.create_index, $4, $6);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		}
	;
index_list:
	/* empty */
	| COMMA ID index_list
	{
		index_append_attribute(&CONTEXT->ssql->sstr.create_index, $2);
		CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length ++;
	}
	;

drop_index:			/*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_DROP_INDEX;//"drop_index";
			drop_index_init(&CONTEXT->ssql->sstr.drop_index, $3, $5);
		}
    ;
create_table:		/*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_CREATE_TABLE;//"create_table";
			// CONTEXT->ssql->sstr.create_table.attribute_count = CONTEXT->value_length;
			create_table_init_name(&CONTEXT->ssql->sstr.create_table, $3);
			//临时变量清零	
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		}
    ;
attr_def_list:
    /* empty */
    | COMMA attr_def attr_def_list {    }
    ;
    
attr_def:
    ID_get type LBRACE number RBRACE 
		{
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, $2, $4, 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
	|ID_get type LBRACE number RBRACE NOT_T V_NULL
		{
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, $2, $4, 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
    |ID_get type LBRACE number RBRACE NULLABLE
		{
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, $2, $4, 1);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
    |ID_get type
		{
			AttrInfo attribute;
			default_attr_info_init(&attribute, CONTEXT->id, $2, 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
	|ID_get type NOT_T V_NULL
		{
			AttrInfo attribute;
			default_attr_info_init(&attribute, CONTEXT->id, $2, 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
	|ID_get type NULLABLE
		{
			AttrInfo attribute;
			default_attr_info_init(&attribute, CONTEXT->id, $2, 1);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
    ;
number:
		POSITIVE_NUMBER {$$ = $1;}
		|MINUS_T POSITIVE_NUMBER {$$ = -$2;}
		;
type:
	INT_T { $$=INTS; }
       | STRING_T { $$=CHARS; }
       | FLOAT_T { $$=FLOATS; }
	   | DATE_T { $$=DATES; }
	   | TEXT_T { $$=TEXTS; }
       ;
ID_get:
	ID 
	{
		char *temp=$1; 
		snprintf(CONTEXT->id, sizeof(CONTEXT->id), "%s", temp);
	}
	;

	
insert:				/*insert   语句的语法解析树*/
    INSERT INTO ID VALUES tuple tuple_list SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_INSERT;//"insert";
			inserts_init(&CONTEXT->ssql->sstr.insertion, $3, CONTEXT->tuples, CONTEXT->tuple_num);
	
	for (int i=0;i<CONTEXT->tuple_num;++i){
		CONTEXT->tuples[i].value_num = 0;
	}

      //临时变量清零
      CONTEXT->tuple_num=0;
    }
	;
tuple_list:
	/* empty */
	| COMMA tuple tuple_list {

	}
	;
tuple:
	LBRACE insert_value insert_value_list RBRACE {
      //临时变量清零
      CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length=0;	
	  CONTEXT->tuple_num ++;
	}
	;
insert_value_list:
    /* empty */
    | COMMA insert_value insert_value_list  { 
  		// CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++] = *$2;
	  }
    ;
insert_value:
    POSITIVE_NUMBER{	
  		value_init_integer(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], $1);
	}
	|MINUS_T POSITIVE_NUMBER{	
  		value_init_integer(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], -$2);
	}
    |POSITIVE_FLOAT{
  		value_init_float(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], $1);
	}
	|MINUS_T POSITIVE_FLOAT{
  		value_init_float(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], -$2);
	}
	|V_NULL{
		value_init_null(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++]);
	}
    |SSS {
			$1 = substr($1,1,strlen($1)-2);
  		value_init_string(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], $1);
	}
    ;
expr:
	ID {
		$$ = create_identifier_expr(NULL, $1);
	}
	| ID DOT ID {
		$$ = create_identifier_expr($1, $3);
	}
	| POSITIVE_NUMBER{
		$$ = create_int_expr($1);
		}
    | POSITIVE_FLOAT{
		$$ = create_float_expr($1);
		}
	| SSS {
		$1 = substr($1,1,strlen($1)-2);
		$$ = create_str_expr($1);
	}
	| V_NULL{
		$$ = create_null_expr();
	}
	| expr PLUS_T expr {
		$$ = create_op_expr(PLUS, $1, $3);
		}
	| expr MINUS_T expr {
		$$ = create_op_expr(MINUS, $1, $3);
		}
	| MINUS_T expr {
		$$ = create_op_expr(MINUS, $2, NULL);
	}
	| PLUS_T expr {
		$$ = create_op_expr(PLUS, $2, NULL);
	}
	| expr STAR expr {
		$$ = create_op_expr(MULTIPLY, $1, $3);
		}
	| expr DIVIDE_T expr {
		$$ = create_op_expr(DIVIDE, $1, $3);
		}
	| LBRACE expr RBRACE {
		$$ = create_brace_expr($2);
	}
	;
value:
    POSITIVE_NUMBER{	
  		value_init_integer(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $1);
		}
    |MINUS_T POSITIVE_NUMBER{	
  		value_init_integer(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], -$2);
		}
    |POSITIVE_FLOAT{
  		value_init_float(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $1);
		}
    |MINUS_T POSITIVE_FLOAT{
  		value_init_float(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], -$2);
		}
    |SSS {
			$1 = substr($1,1,strlen($1)-2);
  		value_init_string(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $1);
		}
    ;
    
delete:		/*  delete 语句的语法解析树*/
    DELETE FROM ID where SEMICOLON 
		{
			CONTEXT->ssql->flag = SCF_DELETE;//"delete";
			deletes_init_relation(&CONTEXT->ssql->sstr.deletion, $3);
			deletes_set_conditions(&CONTEXT->ssql->sstr.deletion,  CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length);
			CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length = 0;	
    }
    ;
update:			/*  update 语句的语法解析树*/
    UPDATE ID SET ID EQ value where SEMICOLON
		{
			CONTEXT->ssql->flag = SCF_UPDATE;//"update";
			Value *value = &CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[0];
			updates_init(&CONTEXT->ssql->sstr.update, $2, $4, value, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length);
			CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length = 0;
		}
    ;
select_:
	select SEMICOLON {
		//临时变量清零
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length=0;
		CONTEXT->from_length=0;
		CONTEXT->select_length=0;
		CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		CONTEXT->ptr = 0;
	}
	;
select:				/*  select 语句的语法解析树*/
    select_sql attr_list FROM ID rel_list inner_join_list where order group
		{
			selects_append_relation(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], $4);
			selects_append_conditions(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length);
			CONTEXT->ssql->flag=SCF_SELECT;		//"select";
			CONTEXT->ptr--;
		}
	;
select_sql:
	SELECT {
		CONTEXT->ssql->select_num++;
		CONTEXT->stack[++CONTEXT->ptr] = CONTEXT->ssql->select_num - 1;
	}
	;

select_attr:
	aggr_op LBRACE other_aggr_attr RBRACE {
		RelAttr attr;
		aggr_relation_attr_init(&attr, $3->relation_name, $3->attribute_name, $3->aggr_value, $1);
		selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		free($3);
		$3 = NULL;
	}
	| COUNT_T LBRACE other_aggr_attr RBRACE {
		RelAttr attr;
		aggr_relation_attr_init(&attr, $3->relation_name, $3->attribute_name, $3->aggr_value, "COUNT");
		selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		free($3);
		$3 = NULL;
	}
	| COUNT_T LBRACE STAR RBRACE {
			RelAttr attr;
			aggr_relation_attr_init(&attr, NULL, "*", -1, "COUNT");
			selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
    | STAR {  
			RelAttr attr;
			relation_attr_init(&attr, NULL, "*");
			selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		}
    | expr {
		RelAttr attr;
		if (get_type($1) == IDENTIFIER_EXPR) {
			relation_attr_init(&attr, get_table_name($1), get_attribute_name($1));
			destroy_expr($1);
		} else {
			attr = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, -1, $1};
		}
		selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
	| ID DOT STAR {
			RelAttr attr;
			relation_attr_init(&attr, $1, "*");
			selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		}
    ;
attr_list:
    select_attr {}
    | select_attr COMMA attr_list {}
  	;

aggr_op:
	MIN_T { $$ = "MIN"; }
	| MAX_T { $$ = "MAX"; }
	| AVG_T { $$ = "AVG"; }
;

other_aggr_attr:
	ID {
		$$ = (RelAttr*) malloc(sizeof(RelAttr));
		*$$ = (RelAttr) {NULL, $1, UNDEFINEDAGGR, -1, NULL};
	}
  	| ID DOT ID {
		$$ = (RelAttr*) malloc(sizeof(RelAttr));
		*$$ = (RelAttr) {$1, $3, UNDEFINEDAGGR, -1, NULL};
	}
	| POSITIVE_NUMBER  {
		$$ = (RelAttr*) malloc(sizeof(RelAttr));
		*$$ = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, $1, NULL};
	}
	| MINUS_T POSITIVE_NUMBER  {
		$$ = (RelAttr*) malloc(sizeof(RelAttr));
		*$$ = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, -$2, NULL};
	}
	| POSITIVE_FLOAT {
		$$ = (RelAttr*) malloc(sizeof(RelAttr));
		*$$ = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, $1, NULL};
	}
	| MINUS_T POSITIVE_FLOAT {
		$$ = (RelAttr*) malloc(sizeof(RelAttr));
		*$$ = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, -$2, NULL};
	}
	;

inner_join_list:
	/* empty */
	| INNER JOIN ID ON condition condition_list inner_join_list {
		selects_append_relation(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], $3);
	}
	;

group:
	/* empty */
	| GROUP BY group_attr group_attr_list{
	}
	;
group_attr:
	ID {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $1);
		selects_append_group(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
	| ID DOT ID {
		RelAttr attr;
		relation_attr_init(&attr, $1, $3);
		selects_append_group(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
	;
group_attr_list:
	/* empty */
    | COMMA group_attr group_attr_list { 
      }
  	;
order:
	/* empty */
	| ORDER BY order_attr order_attr_list {

	};

order_attr:
	ID DOT ID ASC {
		RelAttr attr;
		relation_attr_init(&attr, $1, $3);
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
	| ID ASC {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $1);
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
	| ID DOT ID DESC {
		RelAttr attr;
		relation_attr_init(&attr, $1, $3);
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 0);
	}
	| ID DESC {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $1);
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 0);
	}
	| ID DOT ID {
		RelAttr attr;
		relation_attr_init(&attr, $1, $3);
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
	| ID {
		RelAttr attr;
		relation_attr_init(&attr, NULL, $1);
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
	;

order_attr_list:
	/* empty */
	| COMMA order_attr order_attr_list{

	}
	;

rel_list:
    /* empty */
    | COMMA ID rel_list {	
				selects_append_relation(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], $2);
		  }
    ;
where:
    /* empty */ 
    | WHERE condition condition_list {	
	}
    ;
condition_list:
    /* empty */
    | AND condition condition_list {
	}
    ;
condition:
	expr comOp expr {
		ExprType left_type = get_type($1), right_type = get_type($3);
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		if (left_type == IDENTIFIER_EXPR && is_value_expr($3)) { // ID comOp value
			left_attr = relation_attr_init(left_attr, get_table_name($1), get_attribute_name($1));
			right_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $3);
			left_is_attr = 1;
			right_is_attr = 0;
			destroy_expr($1);
			destroy_expr($3);
		} else if (is_value_expr($1) && is_value_expr($3)) { // value comOp value
			left_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $1);
			right_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $3);
			left_is_attr = 0;
			right_is_attr = 0;
			destroy_expr($1);
			destroy_expr($3);
		} else if (left_type == IDENTIFIER_EXPR && right_type == IDENTIFIER_EXPR) { // ID comOp ID
			left_attr = relation_attr_init(left_attr, get_table_name($1), get_attribute_name($1));
			right_attr = relation_attr_init(right_attr, get_table_name($3), get_attribute_name($3));
			left_is_attr = 1;
			right_is_attr = 1;
			destroy_expr($1);
			destroy_expr($3);
		} else if (is_value_expr($1) && right_type == IDENTIFIER_EXPR) {  // value comOp ID
			left_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], $1);
			right_attr = relation_attr_init(right_attr, get_table_name($3), get_attribute_name($3));
			left_is_attr = 0;
			right_is_attr = 1;
			destroy_expr($1);
			destroy_expr($3);
		} else { // arithmetic expressions is a special kind of value expression
			left_value = &CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++];
			right_value = &CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++];
			left_is_attr = 0;
			right_is_attr = 0;
			left_value->type = ARITHMETIC_EXPR;
			left_value->data = (void*) $1;
			right_value->type = ARITHMETIC_EXPR;
			right_value->data = (void*) $3;
		}
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
		if (left_attr != NULL) {
			free(left_attr);
			left_attr = NULL;
		}
		if (right_attr != NULL) {
			free(right_attr);
			right_attr = NULL;
		}
	}
	| expr comOp LBRACE select RBRACE {
		ExprType left_type = get_type($1);
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		if (left_type == IDENTIFIER_EXPR) {
			left_is_attr = 1;
			left_attr = relation_attr_init(left_attr, get_table_name($1), get_attribute_name($1));
			destroy_expr($1);
		}
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
		if (left_attr != NULL) {
			free(left_attr);
			left_attr = NULL;
		}
	}
	| LBRACE select RBRACE comOp expr {
		ExprType right_type = get_type($5);
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		if (right_type == IDENTIFIER_EXPR) {
			right_is_attr = 1;
			right_attr = relation_attr_init(right_attr, get_table_name($5), get_attribute_name($5));
			destroy_expr($5);
		}
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
		if (right_attr != NULL) {
			free(right_attr);
			right_attr = NULL;
		}
	}

	| LBRACE select RBRACE comOp LBRACE select RBRACE {
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
	}
    ;

comOp:
  	  EQ { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = EQUAL_TO; }
    | LT { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = LESS_THAN; }
    | GT { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = GREAT_THAN; }
    | LE { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = LESS_EQUAL; }
    | GE { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = GREAT_EQUAL; }
    | NE { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = NOT_EQUAL; }
	| IN_T { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = IN; }
	| NOT_T IN_T { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = NOT_IN; }
	| IS NOT_T { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = IS_NOT_NULL; }
	| IS { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = IS_NULL; }
    ;

load_data:
		LOAD DATA INFILE SSS INTO TABLE ID SEMICOLON
		{
		  CONTEXT->ssql->flag = SCF_LOAD_DATA;
			load_data_init(&CONTEXT->ssql->sstr.load_data, $7, $4);
		}
		;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, Query *sqls){
	ParserContext context;
	memset(&context, 0, sizeof(context));
	yyscan_t scanner;
	yylex_init_extra(&context, &scanner);
	context.ssql = sqls;
	scan_string(s, scanner);
	int result = yyparse(scanner);
	yylex_destroy(scanner);
	return result;
}
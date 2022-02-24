/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_YY_YACC_SQL_TAB_H_INCLUDED
# define YY_YY_YACC_SQL_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    SEMICOLON = 258,
    CREATE = 259,
    DROP = 260,
    TABLE = 261,
    TABLES = 262,
    UNIQUE = 263,
    INDEX = 264,
    SELECT = 265,
    INNER = 266,
    JOIN = 267,
    ORDER = 268,
    GROUP = 269,
    BY = 270,
    DESC = 271,
    ASC = 272,
    SHOW = 273,
    SYNC = 274,
    INSERT = 275,
    DELETE = 276,
    UPDATE = 277,
    COMMA = 278,
    TRX_BEGIN = 279,
    TRX_COMMIT = 280,
    TRX_ROLLBACK = 281,
    INT_T = 282,
    TEXT_T = 283,
    STRING_T = 284,
    FLOAT_T = 285,
    DATE_T = 286,
    COUNT_T = 287,
    AVG_T = 288,
    MAX_T = 289,
    MIN_T = 290,
    HELP = 291,
    EXIT = 292,
    DOT = 293,
    INTO = 294,
    VALUES = 295,
    FROM = 296,
    WHERE = 297,
    AND = 298,
    SET = 299,
    ON = 300,
    LOAD = 301,
    DATA = 302,
    INFILE = 303,
    EQ = 304,
    LT = 305,
    GT = 306,
    LE = 307,
    GE = 308,
    NE = 309,
    IN_T = 310,
    NOT_T = 311,
    IS = 312,
    V_NULL = 313,
    NULLABLE = 314,
    PLUS_T = 315,
    MINUS_T = 316,
    STAR = 317,
    DIVIDE_T = 318,
    LBRACE = 319,
    RBRACE = 320,
    POSITIVE_NUMBER = 321,
    POSITIVE_FLOAT = 322,
    ID = 323,
    PATH = 324,
    SSS = 325,
    STRING_V = 326
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 140 "yacc_sql.y"

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

#line 143 "yacc_sql.tab.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (void *scanner);

#endif /* !YY_YY_YACC_SQL_TAB_H_INCLUDED  */

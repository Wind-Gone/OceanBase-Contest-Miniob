/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.5.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 2 "yacc_sql.y"


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


#line 136 "yacc_sql.tab.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
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

#line 274 "yacc_sql.tab.c"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (void *scanner);

#endif /* !YY_YY_YACC_SQL_TAB_H_INCLUDED  */



#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))

/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                            \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  2
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   298

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  72
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  50
/* YYNRULES -- Number of rules.  */
#define YYNRULES  142
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  280

#define YYUNDEFTOK  2
#define YYMAXUTOK   326


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   173,   173,   175,   179,   180,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   199,   204,   209,   215,   221,   227,   233,   239,   245,
     252,   260,   269,   271,   279,   286,   295,   297,   301,   308,
     315,   322,   329,   336,   345,   346,   349,   350,   351,   352,
     353,   356,   365,   378,   380,   385,   391,   393,   398,   401,
     404,   407,   410,   413,   419,   422,   425,   428,   431,   435,
     438,   441,   444,   447,   450,   453,   456,   461,   464,   467,
     470,   473,   480,   489,   498,   508,   517,   524,   531,   538,
     543,   548,   558,   565,   566,   570,   571,   572,   576,   580,
     584,   588,   592,   596,   602,   604,   609,   611,   615,   620,
     626,   628,   631,   633,   638,   643,   648,   653,   658,   663,
     670,   672,   677,   679,   683,   685,   688,   690,   694,   749,
     767,   786,   797,   798,   799,   800,   801,   802,   803,   804,
     805,   806,   810
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 0
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "SEMICOLON", "CREATE", "DROP", "TABLE",
  "TABLES", "UNIQUE", "INDEX", "SELECT", "INNER", "JOIN", "ORDER", "GROUP",
  "BY", "DESC", "ASC", "SHOW", "SYNC", "INSERT", "DELETE", "UPDATE",
  "COMMA", "TRX_BEGIN", "TRX_COMMIT", "TRX_ROLLBACK", "INT_T", "TEXT_T",
  "STRING_T", "FLOAT_T", "DATE_T", "COUNT_T", "AVG_T", "MAX_T", "MIN_T",
  "HELP", "EXIT", "DOT", "INTO", "VALUES", "FROM", "WHERE", "AND", "SET",
  "ON", "LOAD", "DATA", "INFILE", "EQ", "LT", "GT", "LE", "GE", "NE",
  "IN_T", "NOT_T", "IS", "V_NULL", "NULLABLE", "PLUS_T", "MINUS_T", "STAR",
  "DIVIDE_T", "LBRACE", "RBRACE", "POSITIVE_NUMBER", "POSITIVE_FLOAT",
  "ID", "PATH", "SSS", "STRING_V", "$accept", "commands", "command",
  "exit", "help", "sync", "begin", "commit", "rollback", "drop_table",
  "show_tables", "desc_table", "create_index", "index_list", "drop_index",
  "create_table", "attr_def_list", "attr_def", "number", "type", "ID_get",
  "insert", "tuple_list", "tuple", "insert_value_list", "insert_value",
  "expr", "value", "delete", "update", "select_", "select", "select_sql",
  "select_attr", "attr_list", "aggr_op", "other_aggr_attr",
  "inner_join_list", "group", "group_attr", "group_attr_list", "order",
  "order_attr", "order_attr_list", "rel_list", "where", "condition_list",
  "condition", "comOp", "load_data", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_int16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326
};
# endif

#define YYPACT_NINF (-210)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
    -210,    75,  -210,   179,    54,  -210,   -60,     9,    24,    -4,
     -12,   -31,    42,    44,    56,    58,    63,    31,  -210,  -210,
    -210,  -210,  -210,  -210,  -210,  -210,  -210,  -210,  -210,  -210,
    -210,  -210,  -210,  -210,  -210,    81,   -28,  -210,    18,    93,
      38,    40,    74,   117,   145,  -210,    88,    89,   115,  -210,
    -210,  -210,  -210,  -210,   119,  -210,   104,  -210,  -210,  -210,
    -210,    83,    83,  -210,    83,  -210,  -210,   132,  -210,   140,
     155,   138,   125,   141,   113,   159,   203,   163,  -210,  -210,
     169,   168,   143,   144,    15,   174,   -10,   -10,    27,   -45,
      83,    83,    83,    83,   -28,   147,    37,   148,   172,   150,
    -210,   151,   149,    94,   217,   173,   182,    52,   158,  -210,
    -210,   186,   160,   161,  -210,  -210,  -210,   -10,   -10,  -210,
    -210,  -210,   204,   165,  -210,   205,    86,   164,   162,   228,
     -46,   210,     4,    73,   191,  -210,   -48,   229,  -210,  -210,
    -210,   170,  -210,   171,   225,  -210,   148,   175,  -210,  -210,
    -210,  -210,  -210,   -15,   177,   176,  -210,  -210,    65,  -210,
    -210,  -210,   214,   149,   239,   178,  -210,  -210,  -210,  -210,
    -210,  -210,  -210,   190,   192,   105,    94,  -210,    71,  -210,
    -210,  -210,   168,   181,  -210,   204,   234,   168,   205,   244,
     193,  -210,   -18,   184,   227,  -210,  -210,   -46,   188,   210,
    -210,   142,  -210,  -210,     4,   140,   191,  -210,  -210,   251,
     252,  -210,   189,   243,  -210,  -210,  -210,   194,  -210,   196,
     227,   195,   197,   214,  -210,  -210,   116,   199,  -210,  -210,
    -210,   213,   250,   245,  -210,    51,   201,   227,   264,  -210,
       4,   140,  -210,    94,   200,   254,  -210,   212,  -210,   268,
    -210,  -210,   207,   191,    -7,   253,   206,  -210,  -210,  -210,
     225,  -210,  -210,   209,   200,  -210,   235,   255,  -210,   129,
     253,   211,   206,  -210,  -210,  -210,  -210,  -210,   255,  -210
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       2,     0,     1,     0,     0,    86,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     3,    20,
      19,    14,    15,    16,    17,     9,    10,    11,    12,    13,
       8,     5,     7,     6,     4,     0,     0,    18,     0,     0,
       0,     0,     0,     0,     0,    23,     0,     0,     0,    24,
      25,    26,    22,    21,     0,    84,     0,    97,    96,    95,
      69,     0,     0,    90,     0,    66,    67,    64,    68,    91,
      93,     0,     0,     0,     0,     0,     0,     0,    29,    28,
       0,   124,     0,     0,     0,    64,    73,    72,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      27,     0,     0,     0,     0,     0,     0,     0,     0,   100,
     102,    98,     0,     0,    76,    92,    65,    70,    71,    74,
      75,    94,   122,     0,    51,    36,     0,     0,     0,     0,
       0,    53,     0,     0,   126,    82,     0,     0,   101,   103,
      89,     0,    88,     0,   104,    87,     0,     0,    46,    50,
      47,    48,    49,    41,     0,     0,    34,    62,     0,    58,
      60,    63,    56,     0,     0,     0,   132,   133,   134,   135,
     136,   137,   138,     0,   141,     0,     0,   125,     0,    77,
      79,    81,   124,     0,    99,   122,     0,   124,    36,     0,
       0,    43,     0,     0,    32,    59,    61,     0,     0,    53,
      52,     0,   139,   140,     0,   128,   126,    78,    80,     0,
       0,   123,     0,   112,    37,    35,    42,     0,    44,     0,
      32,     0,     0,    56,    55,    54,     0,     0,   127,    83,
     142,     0,     0,   106,    45,    38,     0,    32,     0,    57,
       0,   130,   129,     0,     0,     0,    85,     0,    40,     0,
      33,    30,     0,   126,   119,   120,     0,    39,    31,   131,
     104,   117,   115,     0,     0,   113,   108,   110,   105,   118,
     120,     0,     0,   107,   116,   114,   121,   109,   110,   111
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -210,  -210,  -210,  -210,  -210,  -210,  -210,  -210,  -210,  -210,
    -210,  -210,  -210,  -209,  -210,  -210,    87,   134,  -210,  -210,
    -210,  -210,    82,   120,    59,    90,   -36,  -210,  -210,  -210,
    -210,  -131,  -210,  -210,   198,  -210,   202,    25,  -210,    12,
       8,  -210,    26,    19,   103,  -136,  -203,  -174,    92,  -210
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,     1,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,   222,    29,    30,   147,   125,   219,   153,
     126,    31,   164,   131,   198,   162,    88,   182,    32,    33,
      34,    35,    36,    70,    71,    72,   112,   187,   246,   267,
     273,   233,   255,   265,   144,   104,   177,   134,   175,    37
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      69,   165,   206,   228,    56,    57,    58,    59,    43,   261,
     262,   236,   157,   178,     5,   158,    44,   115,   179,   180,
     159,   160,   181,   116,   161,    86,    87,    45,   250,    47,
      60,   263,    61,    62,    63,    46,    64,    48,    65,    66,
      67,   190,    68,   217,   191,    49,   209,    50,   218,   192,
     260,   213,    92,    93,   117,   118,   119,   120,    69,    51,
      41,    52,    60,    42,    61,    62,    53,   133,    64,   253,
      65,    66,    85,   227,    68,     2,   107,   108,    54,     3,
       4,   109,   110,   111,    55,     5,    73,    90,    91,    92,
      93,     6,   114,     7,     8,     9,    10,    11,   107,    12,
      13,    14,    74,   109,   110,   111,    75,   247,    76,   252,
     248,    15,    16,   148,   149,   150,   151,   152,   138,   139,
      78,    17,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   195,   196,    90,    91,    92,    93,   207,   208,   205,
     133,    60,    77,    61,    62,   274,   275,    64,    79,    65,
      66,    85,    60,    68,    61,    62,    80,    81,   132,    82,
      65,    66,    85,    60,    68,    61,    62,    83,    84,   204,
      89,    65,    66,    85,    60,    68,    61,    62,    94,    95,
     240,    98,    65,    66,    85,    38,    68,    39,    40,    96,
     241,   166,   167,   168,   169,   170,   171,   172,   173,   174,
      90,    91,    92,    93,    99,    97,   100,   133,   101,   102,
     103,   105,   113,   130,   106,   122,   124,   127,   128,   129,
     135,   137,   136,   140,   141,   142,   155,   143,   146,   116,
     145,   156,   154,   163,   176,   183,   186,   197,   184,   185,
     189,   193,   200,   201,   194,   202,   212,   215,   203,   210,
     221,   216,   220,   224,   229,   230,   232,   231,   243,   245,
     234,   235,   238,   237,   242,   244,   249,   251,   254,   256,
     257,   258,   259,   271,   266,   214,   264,   269,   272,   277,
     188,   225,   239,   199,   278,   268,   279,   223,   211,   276,
     270,     0,   121,   226,     0,     0,     0,     0,   123
};

static const yytype_int16 yycheck[] =
{
      36,   132,   176,   206,    32,    33,    34,    35,    68,    16,
      17,   220,    58,    61,    10,    61,     7,    62,    66,    67,
      66,    67,    70,    68,    70,    61,    62,     3,   237,    41,
      58,    38,    60,    61,    62,    39,    64,    68,    66,    67,
      68,    56,    70,    61,    59,     3,   182,     3,    66,    64,
     253,   187,    62,    63,    90,    91,    92,    93,    94,     3,
       6,     3,    58,     9,    60,    61,     3,   103,    64,   243,
      66,    67,    68,   204,    70,     0,    61,    62,    47,     4,
       5,    66,    67,    68,     3,    10,    68,    60,    61,    62,
      63,    16,    65,    18,    19,    20,    21,    22,    61,    24,
      25,    26,     9,    66,    67,    68,    68,    56,    68,   240,
      59,    36,    37,    27,    28,    29,    30,    31,    66,    67,
       3,    46,    49,    50,    51,    52,    53,    54,    55,    56,
      57,    66,    67,    60,    61,    62,    63,    66,    67,   175,
     176,    58,    68,    60,    61,    16,    17,    64,     3,    66,
      67,    68,    58,    70,    60,    61,    68,    68,    64,    44,
      66,    67,    68,    58,    70,    60,    61,    48,    64,    64,
      38,    66,    67,    68,    58,    70,    60,    61,    23,    41,
      64,    68,    66,    67,    68,     6,    70,     8,     9,    64,
     226,    49,    50,    51,    52,    53,    54,    55,    56,    57,
      60,    61,    62,    63,    45,    64,     3,   243,    45,    40,
      42,    68,    38,    64,    70,    68,    68,    45,    68,    68,
       3,    39,    49,    65,    38,    65,    64,    23,    23,    68,
      65,     3,    68,    23,    43,     6,    11,    23,    68,    68,
      65,    64,     3,    65,    68,    55,    12,     3,    56,    68,
      23,    58,    68,    65,     3,     3,    13,    68,    45,    14,
      66,    65,    65,    68,    65,    15,    65,     3,    68,    15,
      58,     3,    65,    38,    68,   188,    23,    68,    23,    68,
     146,   199,   223,   163,   272,   260,   278,   197,   185,   270,
     264,    -1,    94,   201,    -1,    -1,    -1,    -1,    96
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,    73,     0,     4,     5,    10,    16,    18,    19,    20,
      21,    22,    24,    25,    26,    36,    37,    46,    74,    75,
      76,    77,    78,    79,    80,    81,    82,    83,    84,    86,
      87,    93,   100,   101,   102,   103,   104,   121,     6,     8,
       9,     6,     9,    68,     7,     3,    39,    41,    68,     3,
       3,     3,     3,     3,    47,     3,    32,    33,    34,    35,
      58,    60,    61,    62,    64,    66,    67,    68,    70,    98,
     105,   106,   107,    68,     9,    68,    68,    68,     3,     3,
      68,    68,    44,    48,    64,    68,    98,    98,    98,    38,
      60,    61,    62,    63,    23,    41,    64,    64,    68,    45,
       3,    45,    40,    42,   117,    68,    70,    61,    62,    66,
      67,    68,   108,    38,    65,    62,    68,    98,    98,    98,
      98,   106,    68,   108,    68,    89,    92,    45,    68,    68,
      64,    95,    64,    98,   119,     3,    49,    39,    66,    67,
      65,    38,    65,    23,   116,    65,    23,    88,    27,    28,
      29,    30,    31,    91,    68,    64,     3,    58,    61,    66,
      67,    70,    97,    23,    94,   103,    49,    50,    51,    52,
      53,    54,    55,    56,    57,   120,    43,   118,    61,    66,
      67,    70,    99,     6,    68,    68,    11,   109,    89,    65,
      56,    59,    64,    64,    68,    66,    67,    23,    96,    95,
       3,    65,    55,    56,    64,    98,   119,    66,    67,   117,
      68,   116,    12,   117,    88,     3,    58,    61,    66,    90,
      68,    23,    85,    97,    65,    94,   120,   103,   118,     3,
       3,    68,    13,   113,    66,    65,    85,    68,    65,    96,
      64,    98,    65,    45,    15,    14,   110,    56,    59,    65,
      85,     3,   103,   119,    68,   114,    15,    58,     3,    65,
     118,    16,    17,    38,    23,   115,    68,   111,   109,    68,
     114,    38,    23,   112,    16,    17,   115,    68,   111,   112
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int8 yyr1[] =
{
       0,    72,    73,    73,    74,    74,    74,    74,    74,    74,
      74,    74,    74,    74,    74,    74,    74,    74,    74,    74,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    84,    85,    85,    86,    87,    88,    88,    89,    89,
      89,    89,    89,    89,    90,    90,    91,    91,    91,    91,
      91,    92,    93,    94,    94,    95,    96,    96,    97,    97,
      97,    97,    97,    97,    98,    98,    98,    98,    98,    98,
      98,    98,    98,    98,    98,    98,    98,    99,    99,    99,
      99,    99,   100,   101,   102,   103,   104,   105,   105,   105,
     105,   105,   105,   106,   106,   107,   107,   107,   108,   108,
     108,   108,   108,   108,   109,   109,   110,   110,   111,   111,
     112,   112,   113,   113,   114,   114,   114,   114,   114,   114,
     115,   115,   116,   116,   117,   117,   118,   118,   119,   119,
     119,   119,   120,   120,   120,   120,   120,   120,   120,   120,
     120,   120,   121
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     0,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     2,     2,     2,     2,     2,     2,     4,     3,     3,
      10,    11,     0,     3,     6,     8,     0,     3,     5,     7,
       6,     2,     4,     3,     1,     2,     1,     1,     1,     1,
       1,     1,     7,     0,     3,     4,     0,     3,     1,     2,
       1,     2,     1,     1,     1,     3,     1,     1,     1,     1,
       3,     3,     2,     2,     3,     3,     3,     1,     2,     1,
       2,     1,     5,     8,     2,     9,     1,     4,     4,     4,
       1,     1,     3,     1,     3,     1,     1,     1,     1,     3,
       1,     2,     1,     2,     0,     7,     0,     4,     1,     3,
       0,     3,     0,     4,     4,     2,     4,     2,     3,     1,
       0,     3,     0,     3,     0,     3,     0,     3,     3,     5,
       5,     7,     1,     1,     1,     1,     1,     1,     1,     2,
       2,     1,     8
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (scanner, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, scanner); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, void *scanner)
{
  FILE *yyoutput = yyo;
  YYUSE (yyoutput);
  YYUSE (scanner);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yytype], *yyvaluep);
# endif
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, void *scanner)
{
  YYFPRINTF (yyo, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyo, yytype, yyvaluep, scanner);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, int yyrule, void *scanner)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[+yyssp[yyi + 1 - yynrhs]],
                       &yyvsp[(yyi + 1) - (yynrhs)]
                                              , scanner);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, scanner); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
#  else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                yy_state_t *yyssp, int yytoken)
{
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Actual size of YYARG. */
  int yycount = 0;
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[+*yyssp];
      YYPTRDIFF_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
      yysize = yysize0;
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYPTRDIFF_T yysize1
                    = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
                    yysize = yysize1;
                  else
                    return 2;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    /* Don't count the "%s"s in the final size, but reserve room for
       the terminator.  */
    YYPTRDIFF_T yysize1 = yysize + (yystrlen (yyformat) - 2 * yycount) + 1;
    if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
      yysize = yysize1;
    else
      return 2;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, void *scanner)
{
  YYUSE (yyvaluep);
  YYUSE (scanner);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void *scanner)
{
/* The lookahead symbol.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs;

    yy_state_fast_t yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss;
    yy_state_t *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYPTRDIFF_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    goto yyexhaustedlab;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
# undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (&yylval, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 21:
#line 199 "yacc_sql.y"
                   {
        CONTEXT->ssql->flag=SCF_EXIT;//"exit";
    }
#line 1663 "yacc_sql.tab.c"
    break;

  case 22:
#line 204 "yacc_sql.y"
                   {
        CONTEXT->ssql->flag=SCF_HELP;//"help";
    }
#line 1671 "yacc_sql.tab.c"
    break;

  case 23:
#line 209 "yacc_sql.y"
                   {
      CONTEXT->ssql->flag = SCF_SYNC;
    }
#line 1679 "yacc_sql.tab.c"
    break;

  case 24:
#line 215 "yacc_sql.y"
                        {
      CONTEXT->ssql->flag = SCF_BEGIN;
    }
#line 1687 "yacc_sql.tab.c"
    break;

  case 25:
#line 221 "yacc_sql.y"
                         {
      CONTEXT->ssql->flag = SCF_COMMIT;
    }
#line 1695 "yacc_sql.tab.c"
    break;

  case 26:
#line 227 "yacc_sql.y"
                           {
      CONTEXT->ssql->flag = SCF_ROLLBACK;
    }
#line 1703 "yacc_sql.tab.c"
    break;

  case 27:
#line 233 "yacc_sql.y"
                            {
        CONTEXT->ssql->flag = SCF_DROP_TABLE;//"drop_table";
        drop_table_init(&CONTEXT->ssql->sstr.drop_table, (yyvsp[-1].string));
    }
#line 1712 "yacc_sql.tab.c"
    break;

  case 28:
#line 239 "yacc_sql.y"
                          {
      CONTEXT->ssql->flag = SCF_SHOW_TABLES;
    }
#line 1720 "yacc_sql.tab.c"
    break;

  case 29:
#line 245 "yacc_sql.y"
                      {
      CONTEXT->ssql->flag = SCF_DESC_TABLE;
      desc_table_init(&CONTEXT->ssql->sstr.desc_table, (yyvsp[-1].string));
    }
#line 1729 "yacc_sql.tab.c"
    break;

  case 30:
#line 253 "yacc_sql.y"
                {
			CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";
			index_append_attribute(&CONTEXT->ssql->sstr.create_index, (yyvsp[-3].string));
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length ++;
			create_index_init(&CONTEXT->ssql->sstr.create_index, (yyvsp[-7].string), (yyvsp[-5].string));
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		}
#line 1741 "yacc_sql.tab.c"
    break;

  case 31:
#line 261 "yacc_sql.y"
                {
			CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";
			index_append_attribute(&CONTEXT->ssql->sstr.create_index, (yyvsp[-3].string));
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length ++;
			create_unique_index_init(&CONTEXT->ssql->sstr.create_index, (yyvsp[-7].string), (yyvsp[-5].string));
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		}
#line 1753 "yacc_sql.tab.c"
    break;

  case 33:
#line 272 "yacc_sql.y"
        {
		index_append_attribute(&CONTEXT->ssql->sstr.create_index, (yyvsp[-1].string));
		CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length ++;
	}
#line 1762 "yacc_sql.tab.c"
    break;

  case 34:
#line 280 "yacc_sql.y"
                {
			CONTEXT->ssql->flag=SCF_DROP_INDEX;//"drop_index";
			drop_index_init(&CONTEXT->ssql->sstr.drop_index, (yyvsp[-3].string), (yyvsp[-1].string));
		}
#line 1771 "yacc_sql.tab.c"
    break;

  case 35:
#line 287 "yacc_sql.y"
                {
			CONTEXT->ssql->flag=SCF_CREATE_TABLE;//"create_table";
			// CONTEXT->ssql->sstr.create_table.attribute_count = CONTEXT->value_length;
			create_table_init_name(&CONTEXT->ssql->sstr.create_table, (yyvsp[-5].string));
			//临时变量清零	
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		}
#line 1783 "yacc_sql.tab.c"
    break;

  case 37:
#line 297 "yacc_sql.y"
                                   {    }
#line 1789 "yacc_sql.tab.c"
    break;

  case 38:
#line 302 "yacc_sql.y"
                {
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, (yyvsp[-3].number), (yyvsp[-1].number), 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
#line 1800 "yacc_sql.tab.c"
    break;

  case 39:
#line 309 "yacc_sql.y"
                {
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, (yyvsp[-5].number), (yyvsp[-3].number), 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
#line 1811 "yacc_sql.tab.c"
    break;

  case 40:
#line 316 "yacc_sql.y"
                {
			AttrInfo attribute;
			attr_info_init(&attribute, CONTEXT->id, (yyvsp[-4].number), (yyvsp[-2].number), 1);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
#line 1822 "yacc_sql.tab.c"
    break;

  case 41:
#line 323 "yacc_sql.y"
                {
			AttrInfo attribute;
			default_attr_info_init(&attribute, CONTEXT->id, (yyvsp[0].number), 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
#line 1833 "yacc_sql.tab.c"
    break;

  case 42:
#line 330 "yacc_sql.y"
                {
			AttrInfo attribute;
			default_attr_info_init(&attribute, CONTEXT->id, (yyvsp[-2].number), 0);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
#line 1844 "yacc_sql.tab.c"
    break;

  case 43:
#line 337 "yacc_sql.y"
                {
			AttrInfo attribute;
			default_attr_info_init(&attribute, CONTEXT->id, (yyvsp[-1].number), 1);
			create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
			CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++;
		}
#line 1855 "yacc_sql.tab.c"
    break;

  case 44:
#line 345 "yacc_sql.y"
                                {(yyval.number) = (yyvsp[0].number);}
#line 1861 "yacc_sql.tab.c"
    break;

  case 45:
#line 346 "yacc_sql.y"
                                         {(yyval.number) = -(yyvsp[0].number);}
#line 1867 "yacc_sql.tab.c"
    break;

  case 46:
#line 349 "yacc_sql.y"
              { (yyval.number)=INTS; }
#line 1873 "yacc_sql.tab.c"
    break;

  case 47:
#line 350 "yacc_sql.y"
                  { (yyval.number)=CHARS; }
#line 1879 "yacc_sql.tab.c"
    break;

  case 48:
#line 351 "yacc_sql.y"
                 { (yyval.number)=FLOATS; }
#line 1885 "yacc_sql.tab.c"
    break;

  case 49:
#line 352 "yacc_sql.y"
                    { (yyval.number)=DATES; }
#line 1891 "yacc_sql.tab.c"
    break;

  case 50:
#line 353 "yacc_sql.y"
                    { (yyval.number)=TEXTS; }
#line 1897 "yacc_sql.tab.c"
    break;

  case 51:
#line 357 "yacc_sql.y"
        {
		char *temp=(yyvsp[0].string); 
		snprintf(CONTEXT->id, sizeof(CONTEXT->id), "%s", temp);
	}
#line 1906 "yacc_sql.tab.c"
    break;

  case 52:
#line 366 "yacc_sql.y"
                {
			CONTEXT->ssql->flag=SCF_INSERT;//"insert";
			inserts_init(&CONTEXT->ssql->sstr.insertion, (yyvsp[-4].string), CONTEXT->tuples, CONTEXT->tuple_num);
	
	for (int i=0;i<CONTEXT->tuple_num;++i){
		CONTEXT->tuples[i].value_num = 0;
	}

      //临时变量清零
      CONTEXT->tuple_num=0;
    }
#line 1922 "yacc_sql.tab.c"
    break;

  case 54:
#line 380 "yacc_sql.y"
                                 {

	}
#line 1930 "yacc_sql.tab.c"
    break;

  case 55:
#line 385 "yacc_sql.y"
                                                     {
      //临时变量清零
      CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length=0;	
	  CONTEXT->tuple_num ++;
	}
#line 1940 "yacc_sql.tab.c"
    break;

  case 57:
#line 393 "yacc_sql.y"
                                            { 
  		// CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++] = *$2;
	  }
#line 1948 "yacc_sql.tab.c"
    break;

  case 58:
#line 398 "yacc_sql.y"
                   {	
  		value_init_integer(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], (yyvsp[0].number));
	}
#line 1956 "yacc_sql.tab.c"
    break;

  case 59:
#line 401 "yacc_sql.y"
                                {	
  		value_init_integer(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], -(yyvsp[0].number));
	}
#line 1964 "yacc_sql.tab.c"
    break;

  case 60:
#line 404 "yacc_sql.y"
                   {
  		value_init_float(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], (yyvsp[0].floats));
	}
#line 1972 "yacc_sql.tab.c"
    break;

  case 61:
#line 407 "yacc_sql.y"
                               {
  		value_init_float(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], -(yyvsp[0].floats));
	}
#line 1980 "yacc_sql.tab.c"
    break;

  case 62:
#line 410 "yacc_sql.y"
               {
		value_init_null(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++]);
	}
#line 1988 "yacc_sql.tab.c"
    break;

  case 63:
#line 413 "yacc_sql.y"
         {
			(yyvsp[0].string) = substr((yyvsp[0].string),1,strlen((yyvsp[0].string))-2);
  		value_init_string(&CONTEXT->tuples[CONTEXT->tuple_num].values[CONTEXT->tuples[CONTEXT->tuple_num].value_num++], (yyvsp[0].string));
	}
#line 1997 "yacc_sql.tab.c"
    break;

  case 64:
#line 419 "yacc_sql.y"
           {
		(yyval.expr1) = create_identifier_expr(NULL, (yyvsp[0].string));
	}
#line 2005 "yacc_sql.tab.c"
    break;

  case 65:
#line 422 "yacc_sql.y"
                    {
		(yyval.expr1) = create_identifier_expr((yyvsp[-2].string), (yyvsp[0].string));
	}
#line 2013 "yacc_sql.tab.c"
    break;

  case 66:
#line 425 "yacc_sql.y"
                         {
		(yyval.expr1) = create_int_expr((yyvsp[0].number));
		}
#line 2021 "yacc_sql.tab.c"
    break;

  case 67:
#line 428 "yacc_sql.y"
                    {
		(yyval.expr1) = create_float_expr((yyvsp[0].floats));
		}
#line 2029 "yacc_sql.tab.c"
    break;

  case 68:
#line 431 "yacc_sql.y"
              {
		(yyvsp[0].string) = substr((yyvsp[0].string),1,strlen((yyvsp[0].string))-2);
		(yyval.expr1) = create_str_expr((yyvsp[0].string));
	}
#line 2038 "yacc_sql.tab.c"
    break;

  case 69:
#line 435 "yacc_sql.y"
                {
		(yyval.expr1) = create_null_expr();
	}
#line 2046 "yacc_sql.tab.c"
    break;

  case 70:
#line 438 "yacc_sql.y"
                           {
		(yyval.expr1) = create_op_expr(PLUS, (yyvsp[-2].expr1), (yyvsp[0].expr1));
		}
#line 2054 "yacc_sql.tab.c"
    break;

  case 71:
#line 441 "yacc_sql.y"
                            {
		(yyval.expr1) = create_op_expr(MINUS, (yyvsp[-2].expr1), (yyvsp[0].expr1));
		}
#line 2062 "yacc_sql.tab.c"
    break;

  case 72:
#line 444 "yacc_sql.y"
                       {
		(yyval.expr1) = create_op_expr(MINUS, (yyvsp[0].expr1), NULL);
	}
#line 2070 "yacc_sql.tab.c"
    break;

  case 73:
#line 447 "yacc_sql.y"
                      {
		(yyval.expr1) = create_op_expr(PLUS, (yyvsp[0].expr1), NULL);
	}
#line 2078 "yacc_sql.tab.c"
    break;

  case 74:
#line 450 "yacc_sql.y"
                         {
		(yyval.expr1) = create_op_expr(MULTIPLY, (yyvsp[-2].expr1), (yyvsp[0].expr1));
		}
#line 2086 "yacc_sql.tab.c"
    break;

  case 75:
#line 453 "yacc_sql.y"
                             {
		(yyval.expr1) = create_op_expr(DIVIDE, (yyvsp[-2].expr1), (yyvsp[0].expr1));
		}
#line 2094 "yacc_sql.tab.c"
    break;

  case 76:
#line 456 "yacc_sql.y"
                             {
		(yyval.expr1) = create_brace_expr((yyvsp[-1].expr1));
	}
#line 2102 "yacc_sql.tab.c"
    break;

  case 77:
#line 461 "yacc_sql.y"
                   {	
  		value_init_integer(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[0].number));
		}
#line 2110 "yacc_sql.tab.c"
    break;

  case 78:
#line 464 "yacc_sql.y"
                            {	
  		value_init_integer(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], -(yyvsp[0].number));
		}
#line 2118 "yacc_sql.tab.c"
    break;

  case 79:
#line 467 "yacc_sql.y"
                   {
  		value_init_float(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[0].floats));
		}
#line 2126 "yacc_sql.tab.c"
    break;

  case 80:
#line 470 "yacc_sql.y"
                           {
  		value_init_float(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], -(yyvsp[0].floats));
		}
#line 2134 "yacc_sql.tab.c"
    break;

  case 81:
#line 473 "yacc_sql.y"
         {
			(yyvsp[0].string) = substr((yyvsp[0].string),1,strlen((yyvsp[0].string))-2);
  		value_init_string(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[0].string));
		}
#line 2143 "yacc_sql.tab.c"
    break;

  case 82:
#line 481 "yacc_sql.y"
                {
			CONTEXT->ssql->flag = SCF_DELETE;//"delete";
			deletes_init_relation(&CONTEXT->ssql->sstr.deletion, (yyvsp[-2].string));
			deletes_set_conditions(&CONTEXT->ssql->sstr.deletion,  CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length);
			CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length = 0;	
    }
#line 2154 "yacc_sql.tab.c"
    break;

  case 83:
#line 490 "yacc_sql.y"
                {
			CONTEXT->ssql->flag = SCF_UPDATE;//"update";
			Value *value = &CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[0];
			updates_init(&CONTEXT->ssql->sstr.update, (yyvsp[-6].string), (yyvsp[-4].string), value, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length);
			CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length = 0;
		}
#line 2165 "yacc_sql.tab.c"
    break;

  case 84:
#line 498 "yacc_sql.y"
                         {
		//临时变量清零
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length=0;
		CONTEXT->from_length=0;
		CONTEXT->select_length=0;
		CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length = 0;
		CONTEXT->ptr = 0;
	}
#line 2178 "yacc_sql.tab.c"
    break;

  case 85:
#line 509 "yacc_sql.y"
                {
			selects_append_relation(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], (yyvsp[-5].string));
			selects_append_conditions(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions, CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length);
			CONTEXT->ssql->flag=SCF_SELECT;		//"select";
			CONTEXT->ptr--;
		}
#line 2189 "yacc_sql.tab.c"
    break;

  case 86:
#line 517 "yacc_sql.y"
               {
		CONTEXT->ssql->select_num++;
		CONTEXT->stack[++CONTEXT->ptr] = CONTEXT->ssql->select_num - 1;
	}
#line 2198 "yacc_sql.tab.c"
    break;

  case 87:
#line 524 "yacc_sql.y"
                                              {
		RelAttr attr;
		aggr_relation_attr_init(&attr, (yyvsp[-1].other_aggr_attr1)->relation_name, (yyvsp[-1].other_aggr_attr1)->attribute_name, (yyvsp[-1].other_aggr_attr1)->aggr_value, (yyvsp[-3].string));
		selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		free((yyvsp[-1].other_aggr_attr1));
		(yyvsp[-1].other_aggr_attr1) = NULL;
	}
#line 2210 "yacc_sql.tab.c"
    break;

  case 88:
#line 531 "yacc_sql.y"
                                                {
		RelAttr attr;
		aggr_relation_attr_init(&attr, (yyvsp[-1].other_aggr_attr1)->relation_name, (yyvsp[-1].other_aggr_attr1)->attribute_name, (yyvsp[-1].other_aggr_attr1)->aggr_value, "COUNT");
		selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		free((yyvsp[-1].other_aggr_attr1));
		(yyvsp[-1].other_aggr_attr1) = NULL;
	}
#line 2222 "yacc_sql.tab.c"
    break;

  case 89:
#line 538 "yacc_sql.y"
                                     {
			RelAttr attr;
			aggr_relation_attr_init(&attr, NULL, "*", -1, "COUNT");
			selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
#line 2232 "yacc_sql.tab.c"
    break;

  case 90:
#line 543 "yacc_sql.y"
           {  
			RelAttr attr;
			relation_attr_init(&attr, NULL, "*");
			selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		}
#line 2242 "yacc_sql.tab.c"
    break;

  case 91:
#line 548 "yacc_sql.y"
           {
		RelAttr attr;
		if (get_type((yyvsp[0].expr1)) == IDENTIFIER_EXPR) {
			relation_attr_init(&attr, get_table_name((yyvsp[0].expr1)), get_attribute_name((yyvsp[0].expr1)));
			destroy_expr((yyvsp[0].expr1));
		} else {
			attr = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, -1, (yyvsp[0].expr1)};
		}
		selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
#line 2257 "yacc_sql.tab.c"
    break;

  case 92:
#line 558 "yacc_sql.y"
                      {
			RelAttr attr;
			relation_attr_init(&attr, (yyvsp[-2].string), "*");
			selects_append_attribute(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
		}
#line 2267 "yacc_sql.tab.c"
    break;

  case 93:
#line 565 "yacc_sql.y"
                {}
#line 2273 "yacc_sql.tab.c"
    break;

  case 94:
#line 566 "yacc_sql.y"
                                  {}
#line 2279 "yacc_sql.tab.c"
    break;

  case 95:
#line 570 "yacc_sql.y"
              { (yyval.string) = "MIN"; }
#line 2285 "yacc_sql.tab.c"
    break;

  case 96:
#line 571 "yacc_sql.y"
                { (yyval.string) = "MAX"; }
#line 2291 "yacc_sql.tab.c"
    break;

  case 97:
#line 572 "yacc_sql.y"
                { (yyval.string) = "AVG"; }
#line 2297 "yacc_sql.tab.c"
    break;

  case 98:
#line 576 "yacc_sql.y"
           {
		(yyval.other_aggr_attr1) = (RelAttr*) malloc(sizeof(RelAttr));
		*(yyval.other_aggr_attr1) = (RelAttr) {NULL, (yyvsp[0].string), UNDEFINEDAGGR, -1, NULL};
	}
#line 2306 "yacc_sql.tab.c"
    break;

  case 99:
#line 580 "yacc_sql.y"
                    {
		(yyval.other_aggr_attr1) = (RelAttr*) malloc(sizeof(RelAttr));
		*(yyval.other_aggr_attr1) = (RelAttr) {(yyvsp[-2].string), (yyvsp[0].string), UNDEFINEDAGGR, -1, NULL};
	}
#line 2315 "yacc_sql.tab.c"
    break;

  case 100:
#line 584 "yacc_sql.y"
                           {
		(yyval.other_aggr_attr1) = (RelAttr*) malloc(sizeof(RelAttr));
		*(yyval.other_aggr_attr1) = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, (yyvsp[0].number), NULL};
	}
#line 2324 "yacc_sql.tab.c"
    break;

  case 101:
#line 588 "yacc_sql.y"
                                   {
		(yyval.other_aggr_attr1) = (RelAttr*) malloc(sizeof(RelAttr));
		*(yyval.other_aggr_attr1) = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, -(yyvsp[0].number), NULL};
	}
#line 2333 "yacc_sql.tab.c"
    break;

  case 102:
#line 592 "yacc_sql.y"
                         {
		(yyval.other_aggr_attr1) = (RelAttr*) malloc(sizeof(RelAttr));
		*(yyval.other_aggr_attr1) = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, (yyvsp[0].floats), NULL};
	}
#line 2342 "yacc_sql.tab.c"
    break;

  case 103:
#line 596 "yacc_sql.y"
                                 {
		(yyval.other_aggr_attr1) = (RelAttr*) malloc(sizeof(RelAttr));
		*(yyval.other_aggr_attr1) = (RelAttr) {NULL, NULL, UNDEFINEDAGGR, -(yyvsp[0].floats), NULL};
	}
#line 2351 "yacc_sql.tab.c"
    break;

  case 105:
#line 604 "yacc_sql.y"
                                                                    {
		selects_append_relation(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], (yyvsp[-4].string));
	}
#line 2359 "yacc_sql.tab.c"
    break;

  case 107:
#line 611 "yacc_sql.y"
                                             {
	}
#line 2366 "yacc_sql.tab.c"
    break;

  case 108:
#line 615 "yacc_sql.y"
           {
		RelAttr attr;
		relation_attr_init(&attr, NULL, (yyvsp[0].string));
		selects_append_group(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
#line 2376 "yacc_sql.tab.c"
    break;

  case 109:
#line 620 "yacc_sql.y"
                    {
		RelAttr attr;
		relation_attr_init(&attr, (yyvsp[-2].string), (yyvsp[0].string));
		selects_append_group(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr);
	}
#line 2386 "yacc_sql.tab.c"
    break;

  case 111:
#line 628 "yacc_sql.y"
                                       { 
      }
#line 2393 "yacc_sql.tab.c"
    break;

  case 113:
#line 633 "yacc_sql.y"
                                              {

	}
#line 2401 "yacc_sql.tab.c"
    break;

  case 114:
#line 638 "yacc_sql.y"
                      {
		RelAttr attr;
		relation_attr_init(&attr, (yyvsp[-3].string), (yyvsp[-1].string));
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
#line 2411 "yacc_sql.tab.c"
    break;

  case 115:
#line 643 "yacc_sql.y"
                 {
		RelAttr attr;
		relation_attr_init(&attr, NULL, (yyvsp[-1].string));
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
#line 2421 "yacc_sql.tab.c"
    break;

  case 116:
#line 648 "yacc_sql.y"
                         {
		RelAttr attr;
		relation_attr_init(&attr, (yyvsp[-3].string), (yyvsp[-1].string));
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 0);
	}
#line 2431 "yacc_sql.tab.c"
    break;

  case 117:
#line 653 "yacc_sql.y"
                  {
		RelAttr attr;
		relation_attr_init(&attr, NULL, (yyvsp[-1].string));
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 0);
	}
#line 2441 "yacc_sql.tab.c"
    break;

  case 118:
#line 658 "yacc_sql.y"
                    {
		RelAttr attr;
		relation_attr_init(&attr, (yyvsp[-2].string), (yyvsp[0].string));
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
#line 2451 "yacc_sql.tab.c"
    break;

  case 119:
#line 663 "yacc_sql.y"
             {
		RelAttr attr;
		relation_attr_init(&attr, NULL, (yyvsp[0].string));
		selects_append_order(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], &attr, 1);
	}
#line 2461 "yacc_sql.tab.c"
    break;

  case 121:
#line 672 "yacc_sql.y"
                                          {

	}
#line 2469 "yacc_sql.tab.c"
    break;

  case 123:
#line 679 "yacc_sql.y"
                        {	
				selects_append_relation(&CONTEXT->ssql->sstr.selection[CONTEXT->stack[CONTEXT->ptr]], (yyvsp[-1].string));
		  }
#line 2477 "yacc_sql.tab.c"
    break;

  case 125:
#line 685 "yacc_sql.y"
                                     {	
	}
#line 2484 "yacc_sql.tab.c"
    break;

  case 127:
#line 690 "yacc_sql.y"
                                   {
	}
#line 2491 "yacc_sql.tab.c"
    break;

  case 128:
#line 694 "yacc_sql.y"
                        {
		ExprType left_type = get_type((yyvsp[-2].expr1)), right_type = get_type((yyvsp[0].expr1));
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		if (left_type == IDENTIFIER_EXPR && is_value_expr((yyvsp[0].expr1))) { // ID comOp value
			left_attr = relation_attr_init(left_attr, get_table_name((yyvsp[-2].expr1)), get_attribute_name((yyvsp[-2].expr1)));
			right_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[0].expr1));
			left_is_attr = 1;
			right_is_attr = 0;
			destroy_expr((yyvsp[-2].expr1));
			destroy_expr((yyvsp[0].expr1));
		} else if (is_value_expr((yyvsp[-2].expr1)) && is_value_expr((yyvsp[0].expr1))) { // value comOp value
			left_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[-2].expr1));
			right_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[0].expr1));
			left_is_attr = 0;
			right_is_attr = 0;
			destroy_expr((yyvsp[-2].expr1));
			destroy_expr((yyvsp[0].expr1));
		} else if (left_type == IDENTIFIER_EXPR && right_type == IDENTIFIER_EXPR) { // ID comOp ID
			left_attr = relation_attr_init(left_attr, get_table_name((yyvsp[-2].expr1)), get_attribute_name((yyvsp[-2].expr1)));
			right_attr = relation_attr_init(right_attr, get_table_name((yyvsp[0].expr1)), get_attribute_name((yyvsp[0].expr1)));
			left_is_attr = 1;
			right_is_attr = 1;
			destroy_expr((yyvsp[-2].expr1));
			destroy_expr((yyvsp[0].expr1));
		} else if (is_value_expr((yyvsp[-2].expr1)) && right_type == IDENTIFIER_EXPR) {  // value comOp ID
			left_value = init_value_from_expr(&CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++], (yyvsp[-2].expr1));
			right_attr = relation_attr_init(right_attr, get_table_name((yyvsp[0].expr1)), get_attribute_name((yyvsp[0].expr1)));
			left_is_attr = 0;
			right_is_attr = 1;
			destroy_expr((yyvsp[-2].expr1));
			destroy_expr((yyvsp[0].expr1));
		} else { // arithmetic expressions is a special kind of value expression
			left_value = &CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++];
			right_value = &CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].values[CONTEXT->values[CONTEXT->stack[CONTEXT->ptr]].value_length++];
			left_is_attr = 0;
			right_is_attr = 0;
			left_value->type = ARITHMETIC_EXPR;
			left_value->data = (void*) (yyvsp[-2].expr1);
			right_value->type = ARITHMETIC_EXPR;
			right_value->data = (void*) (yyvsp[0].expr1);
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
#line 2551 "yacc_sql.tab.c"
    break;

  case 129:
#line 749 "yacc_sql.y"
                                          {
		ExprType left_type = get_type((yyvsp[-4].expr1));
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		if (left_type == IDENTIFIER_EXPR) {
			left_is_attr = 1;
			left_attr = relation_attr_init(left_attr, get_table_name((yyvsp[-4].expr1)), get_attribute_name((yyvsp[-4].expr1)));
			destroy_expr((yyvsp[-4].expr1));
		}
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
		if (left_attr != NULL) {
			free(left_attr);
			left_attr = NULL;
		}
	}
#line 2574 "yacc_sql.tab.c"
    break;

  case 130:
#line 767 "yacc_sql.y"
                                          {
		ExprType right_type = get_type((yyvsp[0].expr1));
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		if (right_type == IDENTIFIER_EXPR) {
			right_is_attr = 1;
			right_attr = relation_attr_init(right_attr, get_table_name((yyvsp[0].expr1)), get_attribute_name((yyvsp[0].expr1)));
			destroy_expr((yyvsp[0].expr1));
		}
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
		if (right_attr != NULL) {
			free(right_attr);
			right_attr = NULL;
		}
	}
#line 2597 "yacc_sql.tab.c"
    break;

  case 131:
#line 786 "yacc_sql.y"
                                                          {
		RelAttr *left_attr = NULL, *right_attr = NULL;
		Value* left_value = NULL, *right_value = NULL;
		int left_is_attr = 0, right_is_attr = 0;
		Condition condition;
		condition_init(&condition, CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp, left_is_attr, left_attr, left_value, right_is_attr, right_attr, right_value);
		CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].conditions[CONTEXT->condition_list[CONTEXT->stack[CONTEXT->ptr]].condition_length++] = condition;
	}
#line 2610 "yacc_sql.tab.c"
    break;

  case 132:
#line 797 "yacc_sql.y"
             { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = EQUAL_TO; }
#line 2616 "yacc_sql.tab.c"
    break;

  case 133:
#line 798 "yacc_sql.y"
         { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = LESS_THAN; }
#line 2622 "yacc_sql.tab.c"
    break;

  case 134:
#line 799 "yacc_sql.y"
         { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = GREAT_THAN; }
#line 2628 "yacc_sql.tab.c"
    break;

  case 135:
#line 800 "yacc_sql.y"
         { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = LESS_EQUAL; }
#line 2634 "yacc_sql.tab.c"
    break;

  case 136:
#line 801 "yacc_sql.y"
         { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = GREAT_EQUAL; }
#line 2640 "yacc_sql.tab.c"
    break;

  case 137:
#line 802 "yacc_sql.y"
         { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = NOT_EQUAL; }
#line 2646 "yacc_sql.tab.c"
    break;

  case 138:
#line 803 "yacc_sql.y"
               { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = IN; }
#line 2652 "yacc_sql.tab.c"
    break;

  case 139:
#line 804 "yacc_sql.y"
                     { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = NOT_IN; }
#line 2658 "yacc_sql.tab.c"
    break;

  case 140:
#line 805 "yacc_sql.y"
                   { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = IS_NOT_NULL; }
#line 2664 "yacc_sql.tab.c"
    break;

  case 141:
#line 806 "yacc_sql.y"
             { CONTEXT->comps[CONTEXT->stack[CONTEXT->ptr]].comp = IS_NULL; }
#line 2670 "yacc_sql.tab.c"
    break;

  case 142:
#line 811 "yacc_sql.y"
                {
		  CONTEXT->ssql->flag = SCF_LOAD_DATA;
			load_data_init(&CONTEXT->ssql->sstr.load_data, (yyvsp[-1].string), (yyvsp[-4].string));
		}
#line 2679 "yacc_sql.tab.c"
    break;


#line 2683 "yacc_sql.tab.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (scanner, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *, YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (scanner, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, scanner);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, scanner);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;


#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (scanner, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif


/*-----------------------------------------------------.
| yyreturn -- parsing is finished, return the result.  |
`-----------------------------------------------------*/
yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, scanner);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[+*yyssp], yyvsp, scanner);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 816 "yacc_sql.y"

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

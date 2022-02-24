#ifndef __OBSERVER_SQL_EXECUTOR_ARITHMETIC_EXPR_H__
#define __OBSERVER_SQL_EXECUTOR_ARITHMETIC_EXPR_H__

#include <vector>
#include "rc.h"
#include "common/log/log.h"
#include "sql/parser/parse_defs.h"
#include "sql/executor/tuple.h"
#include <unordered_map>
#include <math.h>

class BaseNode {
public:
    ExprType type;
    BaseNode* left;
    BaseNode* right;
    BaseNode(ExprType type): type(type), left(nullptr), right(nullptr) {}
    virtual double evaluate(bool&) = 0;
    virtual ~BaseNode() {};
    virtual std::string to_string() = 0;
};

class ValueNode: public BaseNode {
    double data;
public:
    ValueNode(double data): BaseNode(FLOAT_VALUE_EXPR), data(data) {}
    ~ValueNode() {};
    double evaluate(bool& valid) override { return data; }
    std::string to_string() override {
        int integer = int(data);
        int frac = int(round( (data+1e-7)*100)) %100;
        if (frac != 0 && frac >= 10) {
            return std::to_string(integer) + "." + std::to_string(frac % 10 == 0 ? frac / 10: frac);
        } else if (frac != 0 && frac < 10) {
            return std::to_string(integer) + ".0" + std::to_string(frac);
        } else {
            return std::to_string(integer);
        }
    }
};

class VariableNode: public BaseNode {
public:
    const char *table_name;
    const char *attribute_name;
    double data;
    VariableNode(const char* table_name, const char* attribute_name): 
        BaseNode(IDENTIFIER_EXPR), table_name(table_name ? strdup(table_name): nullptr), attribute_name(strdup(attribute_name)) {}
    ~VariableNode() {
        if (table_name != nullptr) { 
            delete table_name;
            table_name = nullptr;
        }
        delete attribute_name;
        attribute_name = nullptr;
    }
    double evaluate(bool& valid) override { return data; }
    std::string to_string() override {
      if (table_name == nullptr && attribute_name != nullptr)
        return std::string(attribute_name);
      else if (table_name != nullptr && attribute_name != nullptr)
        return std::string(table_name) + "." + std::string(attribute_name);
      else  return "";
    }
};

class OperationNode: public BaseNode {
public:
    OperationNode(ExprType type): BaseNode(type) {}
    ~OperationNode() {
        if (left != nullptr) {
            delete left;
            left = nullptr;
        }
        if (right != nullptr) {
            delete right;
            right = nullptr;
        }
    }
    double evaluate(bool& valid) override;
    std::string to_string() override {
        switch (type) {
            case PLUS: {
                if (right != nullptr) {
                    return left->to_string() + "+" + right->to_string();
                } else {
                    return std::string("+") + left->to_string();
                }
            }  break;
            case MINUS: {
                if (right != nullptr) {
                    return left->to_string() + "-" + right->to_string();
                } else {
                    return std::string("-") + left->to_string();
                }
            }  break;
            case MULTIPLY: {
               return left->to_string() + "*" + right->to_string();
            }  break;
            case DIVIDE: {
               return left->to_string() + "/" + right->to_string();
            }  break;
            default:  break;
        }
    }
};

class BraceExprNode: public BaseNode {
public:
    BraceExprNode(): BaseNode(BRACE_EXPR) {}
    ~BraceExprNode() { 
        if (left != nullptr) {
            delete left;
            left = nullptr;
        }
    }
    double evaluate(bool& valid) { return left->evaluate(valid); }
    std::string to_string() { 
        return std::string("(") + left->to_string() + ")";
    }
};

class Expr {
    BaseNode* root;
    std::unordered_map<std::string, std::vector<VariableNode*>> name2var;
public:
    RC init(void* raw_expr);
    void set_var(const char *table_name, const char* attribue_name, double value);
    TupleValue* evaluate() {
        bool valid = true;
        double val = root->evaluate(valid);
        TupleValue* value = new FloatValue(val, !valid);
        return value;
    }
    std::string to_string() { return root->to_string(); }
    std::unordered_map<std::string, std::vector<VariableNode*>> getExpr() { return name2var; }
};




RC buildExprTree(void* raw_expr, BaseNode*& root, std::unordered_map<std::string, std::vector<VariableNode*>>& variables);
std::string get_canonical_name(const char *table_name, const char *attribute_name);
#endif
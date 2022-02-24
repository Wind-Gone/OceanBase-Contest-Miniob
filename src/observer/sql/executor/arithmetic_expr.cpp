#include "sql/executor/arithmetic_expr.h"

double OperationNode::evaluate(bool& valid) {
    switch (type)
    {
    case PLUS:
        if (right != nullptr) {
            return left->evaluate(valid) + right->evaluate(valid);
        } else {
            return left->evaluate(valid);
        }
        break;
    case MINUS:
        if (right != nullptr) {
            return left->evaluate(valid) - right->evaluate(valid);
        } else {
            return -left->evaluate(valid);
        }
        break;
    case MULTIPLY:
        return left->evaluate(valid) * right->evaluate(valid);
        break;
    case DIVIDE: {
        double right_value = right->evaluate(valid);
        if (right_value == 0.0) {
            valid = false;
            return 0.0;
        } else {
            return left->evaluate(valid) / right->evaluate(valid);
        }
    } break;
    default:
        break;
    }
    LOG_PANIC("Illegal operator in evaluation");
    return 0.0; // never run here;
}

RC Expr::init(void* raw_expr) {
    RC rc = buildExprTree(raw_expr, root, name2var);
    if (rc != RC::SUCCESS) {
        LOG_ERROR("expression initialization failed");
    }
    // TODO check variable validaty
    return rc;
}

void Expr::set_var(const char *table_name, const char* attribue_name, double new_value) {
    const std::vector<VariableNode*>& vars = name2var[get_canonical_name(table_name, attribue_name)];
    for (VariableNode* node: vars) {
        node->data = new_value;
    }
}

// raw_expr: raw expression not checked
// root: current expression tree root
// variables: all variables now encounterd
RC buildExprTree(void* raw_expr, BaseNode*& root, 
        std::unordered_map<std::string, std::vector<VariableNode*>>& variables) {
    RC rc = RC::SUCCESS;
    ExprType type = get_type(raw_expr);
    switch (type)
    {
    case PLUS:
    case MINUS:
    case MULTIPLY:
    case DIVIDE: {
        OperationNode* op_node = new OperationNode(type);
        root = op_node;
        rc = buildExprTree(((OpNode*) raw_expr)->left, op_node->left, variables);
        if (rc != RC::SUCCESS) return rc;
        if (((OpNode*) raw_expr)->right != NULL) {
            rc = buildExprTree(((OpNode*) raw_expr)->right, op_node->right, variables);
            if (rc != RC::SUCCESS) return rc;
        }
        break;
    }
    case INT_VALUE_EXPR:
    case FLOAT_VALUE_EXPR: {
        double value = ((NumericalValueNode*) raw_expr)->value;
        ValueNode* value_node = new ValueNode(value);
        root = value_node;
        break;
    }
    case STR_VALUE_EXPR: {
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
    case IDENTIFIER_EXPR: {
        IdentifierNode* node = (IdentifierNode*) raw_expr;
        VariableNode* variable_node = new VariableNode(node->table_name, node->attribute_name);
        root = variable_node;
        variables[get_canonical_name(variable_node->table_name, variable_node->attribute_name)].emplace_back(variable_node);
        break;
    }
    case BRACE_EXPR: {
        BraceNode* node = (BraceNode*) raw_expr;
        BraceExprNode* expr = new BraceExprNode();
        root = expr;
        rc = buildExprTree(node->expr, expr->left, variables);
        if (rc != RC::SUCCESS) return rc;
        break;
    }
    
    default:
        break;
    }
    return rc;
}


std::string get_canonical_name(const char *table_name, const char *attribute_name) {
    if (table_name == nullptr) return std::string(attribute_name);
    return std::string(table_name) + "." + std::string(attribute_name);
}
"""Safe evaluation of configuration expressions (DQ `expression` rules, `when` clauses).

Business users / data stewards can add cross-field rules in YAML, e.g.
    "country_code != 'US' or (len(postal_code) == 5 and postal_code.isdigit())"
without a code deployment. Expressions are parsed to an AST and validated
against a whitelist BEFORE evaluation: no imports, no attribute access to
dunders, no arbitrary function calls - only comparisons, boolean logic,
literals, record fields, and a small set of safe functions/str methods.
"""
from __future__ import annotations

import ast
import re

_ALLOWED_NODES = (
    ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.UnaryOp, ast.Not, ast.Compare, ast.Eq, ast.NotEq,
    ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.In, ast.NotIn, ast.Is, ast.IsNot, ast.Name, ast.Load,
    ast.Constant, ast.Tuple, ast.List, ast.Call, ast.Attribute, ast.BinOp, ast.Add, ast.Sub, ast.IfExp,
)
_ALLOWED_FUNCS = {"len": len, "str": str, "int": int, "abs": abs, "bool": bool,
                  "matches": lambda v, p: v is not None and re.fullmatch(p, v) is not None}
_ALLOWED_METHODS = {"isdigit", "isalpha", "startswith", "endswith", "lower", "upper", "strip"}


class UnsafeExpression(ValueError):
    pass


def compile_expression(expr: str):
    tree = ast.parse(expr, mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise UnsafeExpression(f"node {type(node).__name__} not allowed in '{expr}'")
        if isinstance(node, ast.Attribute) and node.attr not in _ALLOWED_METHODS:
            raise UnsafeExpression(f"attribute '{node.attr}' not allowed in '{expr}'")
        if isinstance(node, ast.Call):
            f = node.func
            if isinstance(f, ast.Name) and f.id not in _ALLOWED_FUNCS:
                raise UnsafeExpression(f"function '{f.id}' not allowed in '{expr}'")
        if isinstance(node, ast.Name) and node.id.startswith("__"):
            raise UnsafeExpression("dunder names not allowed")
    return compile(tree, "<rule>", "eval")


def evaluate(code, record: dict) -> bool:
    env = {"__builtins__": {}, **_ALLOWED_FUNCS}
    names = {k: (None if (isinstance(v, float) and v != v) else v) for k, v in record.items()}
    try:
        return bool(eval(code, env, names))  # noqa: S307 - AST whitelisted above
    except (TypeError, AttributeError, NameError):
        return False

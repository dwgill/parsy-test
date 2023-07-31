from dataclasses import dataclass
from typing import Callable, Generic, Iterable, TypeVar
from abc import ABC
from parsy import regex, seq, string, string_from
import json

# Docs:
# https://parsy.readthedocs.io/en/latest/ref/primitives.html
# https://parsy.readthedocs.io/en/latest/ref/methods_and_combinators.html

# Matches a JSON-encoded string, i.e. wraped in "", with interior quotes escaped \" etc.
# Adapted from: https://regex101.com/library/tA9pM8
JSON_STR_RE = r'"(\\(["\\\/bfnrt]|u[a-fA-F0-9]{4})|[^"\\\0-\x1F\x7F]+)*"'

# The regex for like a table name or similar
IDENTIFIER_RE = r"[a-zA-Z_][a-zA-Z0-9_]*"

T = TypeVar("T")
K = TypeVar("K")
ExprVal = TypeVar("ExprVal", bound=str | float | int | bool | None)


class AbstractExpr(ABC, Generic[ExprVal]):
    value: tuple[ExprVal, ...]

    def is_empty(self):
        return len(self.value) == 0

    def is_single(self):
        return len(self.value) == 1

    def is_many(self):
        return len(self.value) != 1


def map_ignore_none(
    f: Callable[[T], K], iterable: Iterable[T | None]
) -> Iterable[K | None]:
    for item in iterable:
        if item is None:
            yield None
        else:
            yield f(item)


def unique_sort_list(exprs: list[ExprVal]):
    return sorted(set(exprs), key=lambda v: v is not None)


# AST Nodes


@dataclass(frozen=True, slots=True)
class TableReference:
    table: str
    column: str


@dataclass(frozen=True, slots=True)
class NumExpr(AbstractExpr):
    value: tuple[float | None, ...] | tuple[int | None, ...]


@dataclass(frozen=True, slots=True)
class StrExpr(AbstractExpr):
    value: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BoolNullExpr(AbstractExpr):
    value: tuple[bool | None, ...]


Expr = NumExpr | StrExpr | BoolNullExpr


@dataclass(frozen=True, slots=True)
class TablePredicate:
    table_reference: TableReference
    expr: Expr


@dataclass(frozen=True, slots=True)
class TablePredicateList:
    predicates: tuple[TablePredicate, ...]


# Parsers

null_token = string("null", transform=lambda s: s.lower()).map(lambda s: None)

table_reference = (
    seq(
        # The keyword args' order matters
        # The keyword args' names correspond to the fields of the TableReference class
        # Keywords with leading underscore _ are discarded
        table=regex(IDENTIFIER_RE).desc("table name"),
        _dot=string(".").desc("table name/column delimiter"),
        column=regex(IDENTIFIER_RE).desc("table column"),
    )
    .combine_dict(TableReference)
    .desc("table.column reference")
)

float_token = regex(r"[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?").map(float).desc("number")
num_or_null_token = float_token | null_token

num_or_null_token_list = (
    num_or_null_token.sep_by(string(","), min=1)
    .map(
        lambda f_list: list(map_ignore_none(int, f_list))
        if all(f is None or f.is_integer() for f in f_list)
        else f_list
    )
    .desc("comma delimited list of ints or floats")
)

str_token = (
    regex(JSON_STR_RE)
    .map(lambda str: json.loads(str))
    .desc("valid, JSON-encoded string")
)  # Need to scrutinize what kinds of strings are valid

str_or_none = str_token | null_token

str_or_null_token_list = str_or_none.sep_by(string(","), min=1).desc(
    "comma delimited list of valid, JSON-encoded strings"
)

bool_token = (
    string_from("true", "false", transform=lambda s: s.lower())
    .map(lambda val: val == "true")
    .desc("true/false/null")
)

bool_or_null = bool_token | null_token

bool_null_token_list = bool_or_null.sep_by(string(","), min=1).desc(
    "comma delimited list of true/false/null"
)

# Convert the list from the parser to a tuple
num_expr = num_or_null_token_list.map(
    lambda exprs: NumExpr(tuple(unique_sort_list(exprs)))
).desc("number list expression")
str_expr = str_or_null_token_list.map(
    lambda exprs: StrExpr(tuple(unique_sort_list(exprs)))
).desc("string list expression")
bool_expr = bool_null_token_list.map(
    lambda exprs: BoolNullExpr(tuple(unique_sort_list(exprs)))
).desc("true/false list expression")

expr = (str_expr | num_expr | bool_expr).desc("expression")

predicate = (
    seq(
        # See comments on the above seq()
        table_reference=table_reference,
        _colon=string(":"),
        expr=expr,
    )
    .combine_dict(TablePredicate)
    .desc("table.column predicate")
)

predicate_list = (
    predicate.sep_by(string(";"), min=1)
    .map(lambda predicates: TablePredicateList(tuple(predicates)))
    .desc("semicolon delimited list of predicates")
)

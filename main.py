from dataclasses import dataclass
from parsy import regex, seq, string, seq, Parser

# Docs:
# https://parsy.readthedocs.io/en/latest/ref/primitives.html
# https://parsy.readthedocs.io/en/latest/ref/methods_and_combinators.html


# AST Nodes


@dataclass(frozen=True, slots=True)
class TableReference:
    table: str
    column: str


@dataclass(frozen=True, slots=True)
class NumExpr:
    val: tuple[int, ...] | tuple[float, ...]


@dataclass(frozen=True, slots=True)
class StrExpr:
    val: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TablePredicate:
    ref: TableReference
    expr: NumExpr | StrExpr


@dataclass(frozen=True, slots=True)
class TablePredicateList:
    preds: tuple[TablePredicate]


# Parsers


def delimited_list(content: Parser, delimiter: str):
    # The built-in sep_by() combinator doesn't work for our purposes because it often partially
    # succeeds if you give it a list whose initial value matches the given parser but later
    # elements don't match. This combinator will only succeed if the list terminates with an
    # item which matches the given parser. In essence, it's a "greedy" version of
    # sep_by()

    return (content << string(delimiter)).many() + content.map(lambda x: [x])


# The regex for like a table name or similar
IDENTIFIER_RE = r"[a-zA-Z_][a-zA-Z0-9_]+"

table_reference = (
    seq(
        # The keyword args' order matters
        # The keyword args' names correspond to the fields of the TableReference class
        # Keywords with leading underscore _ are discarded
        table=regex(IDENTIFIER_RE).desc("table name"),
        _dot=string("."),
        column=regex(IDENTIFIER_RE).desc("table column"),
    )
    .desc("table.column reference")
    .combine_dict(TableReference)
)

float_token = regex(r"[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?").map(float).desc("number")

float_token_list = delimited_list(float_token, ",").map(tuple)

num_token_list = (
    float_token_list.map(
        # Convert to a int tuple if all numbers are ints
        lambda tpl: tuple(map(int, tpl))
        if all(f.is_integer() for f in tpl)
        else tpl
    )
    .desc("comma delimited list of numbers")
    .map(NumExpr)
)

text_token = regex(r"[^,;:]+").desc("string terminated by a comma")

text_token_list = (
    delimited_list(text_token, ",")
    .desc("comma delimited list of strings")
    .map(tuple)
    .map(StrExpr)
)

value_token_list = (num_token_list | text_token_list).desc(
    "comma delimited list of strings or numbers"
)

table_predicate = (
    seq(
        ref=table_reference,
        _colon=string(":"),
        expr=value_token_list,
    )
    .desc("a table.column:value1,value2 table predicate")
    .combine_dict(TablePredicate)
)

table_predicate_list = (
    delimited_list(table_predicate, ";")
    .desc("a semicolon delimited list of table.column:value1,value2 predicates")
    .map(tuple)
    .map(TablePredicateList)
)

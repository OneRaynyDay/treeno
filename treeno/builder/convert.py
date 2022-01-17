"""
Converts from our grammar into a buildable query tree.
"""
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from overrides import overrides

from treeno.datatypes.builder import (
    array,
    boolean,
    double,
    integer,
    interval,
    row,
    unknown,
    varchar,
)
from treeno.datatypes.inference import infer_decimal, infer_integral
from treeno.datatypes.types import FIELDS, TIME, TIMESTAMP, DataType
from treeno.expression import (
    AliasedStar,
    AliasedValue,
    And,
    Array,
    Between,
    Case,
    Cast,
    DistinctFrom,
    Else,
    Field,
    InList,
    InQuery,
    Interval,
    IsNull,
    Lambda,
    Like,
    Literal,
    Or,
    RowConstructor,
    Star,
    Subscript,
    TryCast,
    TypeConstructor,
    Value,
    When,
)
from treeno.functions.aggregate import (
    AggregateFunction,
    CountIndication,
    ListAgg,
    OverflowFiller,
)
from treeno.functions.base import NAMES_TO_FUNCTIONS, Function
from treeno.functions.common import Concatenate
from treeno.functions.session import (
    CurrentCatalog,
    CurrentPath,
    CurrentSchema,
    CurrentUser,
)
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.grammar.gen.SqlBaseVisitor import SqlBaseVisitor
from treeno.grammar.parse import AST
from treeno.groupby import Cube, GroupBy, GroupingSet, GroupingSetList, Rollup
from treeno.orderby import NullOrder, OrderTerm, OrderType
from treeno.relation import (
    AliasedRelation,
    ExceptQuery,
    IntersectQuery,
    Join,
    JoinConfig,
    JoinCriteria,
    JoinOnCriteria,
    JoinType,
    JoinUsingCriteria,
    Lateral,
    Query,
    Relation,
    SampleType,
    SelectQuery,
    SetQuantifier,
    SetQuery,
    Table,
    TableQuery,
    TableSample,
    UnionQuery,
    Unnest,
    ValuesQuery,
)
from treeno.util import nth
from treeno.window import (
    BoundedFrameBound,
    BoundType,
    CurrentFrameBound,
    FrameType,
    NullTreatment,
    UnboundedFrameBound,
    Window,
)


def query_from_sql(sql: str) -> Query:
    return ConvertVisitor().visitQuery(AST(sql).query())


def expression_from_sql(sql: str) -> Value:
    return ConvertVisitor().visitStandaloneExpression(AST(sql).expression())


def type_from_sql(sql: str) -> DataType:
    return ConvertVisitor().visitStandaloneType(AST(sql).type())


def apply_operator(operator: str, *args: Value) -> Value:
    if operator == "AND":
        return args[0] & args[1]
    elif operator == "OR":
        return args[0] | args[1]
    elif operator == "=":
        return args[0] == args[1]
    elif operator == ">":
        return args[0] > args[1]
    elif operator == ">=":
        return args[0] >= args[1]
    elif operator == "<":
        return args[0] < args[1]
    elif operator == "<=":
        return args[0] <= args[1]
    elif operator == "<>":
        return args[0] != args[1]
    elif operator == "+":
        if len(args) == 1:
            return +args[0]
        else:
            assert len(args) == 2
            return args[0] + args[1]
    elif operator == "-":
        if len(args) == 1:
            return -args[0]
        else:
            assert len(args) == 2
            return args[0] - args[1]
    elif operator == "*":
        return args[0] * args[1]
    elif operator == "/":
        return args[0] / args[1]
    elif operator == "%":
        return args[0] % args[1]
    else:
        raise NotImplementedError(f"Unrecognized token {operator}")


def table_from_qualifiers(qualifiers: List[str]) -> Table:
    qualifiers = list(reversed(qualifiers))
    name: str = qualifiers[0]
    schema: Optional[str] = nth(qualifiers, 1)
    catalog: Optional[str] = nth(qualifiers, 2)

    return Table(name=name, schema=schema, catalog=catalog)


class ConvertVisitor(SqlBaseVisitor):
    """Converts the tree into a builder tree in python
    """

    @overrides
    def visitSingleStatement(
        self, ctx: SqlBaseParser.SingleStatementContext
    ) -> Query:
        stmt = ctx.statement()
        if not isinstance(stmt, SqlBaseParser.StatementDefaultContext):
            raise NotImplementedError(
                "Only standard selects are implemented for now"
            )
        return self.visit(ctx.statement())

    @overrides
    def visitStandaloneExpression(
        self, ctx: SqlBaseParser.StandaloneExpressionContext
    ) -> Value:
        return self.visit(ctx.expression())

    @overrides
    def visitStandaloneType(
        self, ctx: SqlBaseParser.StandaloneTypeContext
    ) -> DataType:
        return self.visit(ctx.type_())

    @overrides
    def visitStatementDefault(self, ctx: SqlBaseParser.StatementDefaultContext):
        return self.visit(ctx.query())

    @overrides
    def visitQuery(self, ctx: SqlBaseParser.QueryContext) -> Query:
        query = self.visit(ctx.queryNoWith())
        with_ = ctx.with_()
        if with_:
            named_queries = self.visit(with_)
            query.with_ = named_queries
        return query

    @overrides
    def visitWith_(
        self, ctx: SqlBaseParser.With_Context
    ) -> List[AliasedRelation]:
        assert (
            not ctx.RECURSIVE()
        ), "Recursive with queries currently not supported"
        return [self.visit(named_query) for named_query in ctx.namedQuery()]

    @overrides
    def visitNamedQuery(
        self, ctx: SqlBaseParser.NamedQueryContext
    ) -> AliasedRelation:
        column_aliases = None
        if ctx.columnAliases():
            column_aliases = self.visit(ctx.columnAliases())
        return AliasedRelation(
            relation=self.visit(ctx.query()),
            alias=self.visit(ctx.name),
            column_aliases=column_aliases,
        )

    @overrides
    def visitQueryNoWith(self, ctx: SqlBaseParser.QueryNoWithContext) -> Query:
        query = self.visit(ctx.queryTerm())
        if ctx.ORDER() and ctx.BY():
            query.orderby = [self.visit(item) for item in ctx.sortItem()]
        if ctx.offset:
            query.offset = self.visit(ctx.offset)
        limit_clause = ctx.limitRowCount()
        if limit_clause:
            # TODO: Assign this to the query object
            query.limit = self.visit(limit_clause)
        return query

    @overrides
    def visitSetOperation(
        self, ctx: SqlBaseParser.SetOperationContext
    ) -> SetQuery:
        left_query = self.visit(ctx.left)
        right_query = self.visit(ctx.right)
        kwargs = {}
        if ctx.setQuantifier():
            kwargs["set_quantifier"] = self.visit(ctx.setQuantifier())
        operator_text = ctx.operator.text.upper()
        if operator_text == "INTERSECT":
            return IntersectQuery(left_query, right_query, **kwargs)
        elif operator_text == "UNION":
            return UnionQuery(left_query, right_query, **kwargs)
        elif operator_text == "EXCEPT":
            return ExceptQuery(left_query, right_query, **kwargs)
        raise NotImplementedError(f"Unsupported operator type {operator_text}")

    @overrides
    def visitRowCount(self, ctx: SqlBaseParser.RowCountContext) -> int:
        assert (
            not ctx.QUESTION_MARK()
        ), "Question mark (?) as a row count is currently not supported"
        return int(ctx.INTEGER_VALUE().getText())

    @overrides
    def visitSortItem(self, ctx: SqlBaseParser.SortItemContext) -> OrderTerm:
        value = self.visit(ctx.expression())
        order_type = (
            OrderType.ASC
            if not ctx.ordering or ctx.ordering.text.upper() == "ASC"
            else OrderType.DESC
        )
        null_order = (
            NullOrder.LAST
            if not ctx.nullOrdering or ctx.nullOrdering.text.upper() == "LAST"
            else NullOrder.FIRST
        )
        return OrderTerm(value, order_type, null_order)

    @overrides
    def visitLimitRowCount(
        self, ctx: SqlBaseParser.LimitRowCountContext
    ) -> Optional[int]:
        count = ctx.getText()
        if count.isnumeric():
            return int(count)
        else:
            assert (
                count == "ALL"
            ), "LIMIT quantities can either be numeric or ALL"
            return None

    @overrides
    def visitQueryTermDefault(
        self, ctx: SqlBaseParser.QueryTermDefaultContext
    ) -> SelectQuery:
        return self.visit(ctx.queryPrimary())

    @overrides
    def visitTable(self, ctx: SqlBaseParser.TableContext) -> TableQuery:
        return TableQuery(
            table=table_from_qualifiers(self.visit(ctx.qualifiedName()))
        )

    @overrides
    def visitInlineTable(
        self, ctx: SqlBaseParser.InlineTableContext
    ) -> ValuesQuery:
        return ValuesQuery(
            exprs=[self.visit(expr) for expr in ctx.expression()]
        )

    @overrides
    def visitQueryPrimaryDefault(
        self, ctx: SqlBaseParser.QueryPrimaryDefaultContext
    ) -> Relation:
        query_spec = ctx.querySpecification()
        if not isinstance(query_spec, SqlBaseParser.QuerySpecificationContext):
            raise NotImplementedError(
                "Directly referencing tables without a SELECT clause is not yet implemented"
            )
        return self.visit(query_spec)

    @overrides
    def visitSetQuantifier(
        self, ctx: SqlBaseParser.SetQuantifierContext
    ) -> SetQuantifier:
        return SetQuantifier[ctx.getText()]

    @overrides
    def visitAnd_(self, ctx: SqlBaseParser.And_Context) -> And:
        return And(
            self.visit(ctx.booleanExpression(0)),
            self.visit(ctx.booleanExpression(1)),
        )

    @overrides
    def visitOr_(self, ctx: SqlBaseParser.Or_Context) -> Or:
        return Or(
            self.visit(ctx.booleanExpression(0)),
            self.visit(ctx.booleanExpression(1)),
        )

    @overrides
    def visitLogicalNot(self, ctx: SqlBaseParser.LogicalNotContext) -> Value:
        return ~self.visit(ctx.booleanExpression())

    @overrides
    def visitPredicated(self, ctx: SqlBaseParser.PredicatedContext) -> Value:
        value = self.visit(ctx.valueExpression())
        # If predicate is not passed in, then the boolean expression is really a value expression.
        predicate = ctx.predicate()
        if predicate:
            # HACK: temporarily assign the value as a member to predicate
            predicate.left_value = value
            value = self.visit(predicate)
            predicate.left_value = None
        return value

    @overrides
    def visitComparison(
        self,
        ctx: SqlBaseParser.ComparisonContext,
        left_value: Optional[Value] = None,
    ) -> Value:
        """Visits a comparison between two expressions. However, the grammar here only contains
        the right expression, so we have a left_value member added to the context.
        """
        right_value = self.visit(ctx.right)
        return apply_operator(
            ctx.comparisonOperator().getText(), ctx.left_value, right_value
        )

    @overrides
    def visitQuantifiedComparison(
        self, ctx: SqlBaseParser.QuantifiedComparisonContext
    ) -> None:
        raise NotImplementedError("Quantified comparison is not yet supported")

    @overrides
    def visitInSubquery(self, ctx: SqlBaseParser.InSubqueryContext) -> Value:
        in_query = InQuery(value=ctx.left_value, query=self.visit(ctx.query()))
        if ctx.NOT():
            return ~in_query
        return in_query

    @overrides
    def visitBetween(self, ctx: SqlBaseParser.BetweenContext) -> Value:
        between = Between(
            ctx.left_value,
            lower=self.visit(ctx.lower),
            upper=self.visit(ctx.upper),
        )
        if ctx.NOT():
            return ~between
        return between

    @overrides
    def visitInList(self, ctx: SqlBaseParser.InListContext) -> Value:
        expressions = ctx.expression()
        in_list = InList(
            value=ctx.left_value,
            exprs=[self.visit(expr) for expr in expressions],
        )
        if ctx.NOT():
            return ~in_list
        return in_list

    @overrides
    def visitLike(self, ctx: SqlBaseParser.LikeContext) -> Value:
        escape = None
        if ctx.escape:
            escape = self.visit(ctx.escape)
        like = Like(
            value=ctx.left_value, pattern=self.visit(ctx.pattern), escape=escape
        )
        if ctx.NOT():
            return ~like
        return like

    @overrides
    def visitNullPredicate(
        self, ctx: SqlBaseParser.NullPredicateContext
    ) -> Value:
        is_null = IsNull(ctx.left_value)
        if ctx.NOT():
            return ~is_null
        return is_null

    @overrides
    def visitDistinctFrom(
        self, ctx: SqlBaseParser.DistinctFromContext
    ) -> Value:
        distinct = DistinctFrom(ctx.left_value, right=self.visit(ctx.right))
        if ctx.NOT():
            return ~distinct
        return distinct

    @overrides
    def visitArithmeticBinary(
        self, ctx: SqlBaseParser.ArithmeticBinaryContext
    ) -> Value:
        left = self.visit(ctx.left)
        right = self.visit(ctx.right)
        return apply_operator(ctx.operator.text.upper(), left, right)

    @overrides
    def visitArithmeticUnary(
        self, ctx: SqlBaseParser.ArithmeticUnaryContext
    ) -> Value:
        return apply_operator(
            ctx.operator.text.upper(), self.visit(ctx.valueExpression())
        )

    @overrides
    def visitConcatenation(
        self, ctx: SqlBaseParser.ConcatenationContext
    ) -> Concatenate:
        return Concatenate(values=[self.visit(ctx.left), self.visit(ctx.right)])

    @overrides
    def visitSimpleCase(self, ctx: SqlBaseParser.SimpleCaseContext) -> Case:
        whens = [self.visit(when_) for when_ in ctx.whenClause()]
        value = self.visit(ctx.operand)
        else_ = (
            Else(self.visit(ctx.elseExpression)) if ctx.elseExpression else None
        )
        return Case(branches=whens, else_=else_, value=value)

    @overrides
    def visitSearchedCase(self, ctx: SqlBaseParser.SearchedCaseContext) -> Case:
        whens = [self.visit(when_) for when_ in ctx.whenClause()]
        else_ = (
            Else(self.visit(ctx.elseExpression)) if ctx.elseExpression else None
        )
        return Case(branches=whens, else_=else_)

    @overrides
    def visitWhenClause(self, ctx: SqlBaseParser.WhenClauseContext) -> When:
        return When(
            condition=self.visit(ctx.condition), value=self.visit(ctx.result)
        )

    @overrides
    def visitCast(self, ctx: SqlBaseParser.CastContext) -> Union[Cast, TryCast]:
        expr = self.visit(ctx.expression())
        output_type = self.visit(ctx.type_())
        if ctx.CAST():
            return Cast(expr=expr, data_type=output_type)
        if ctx.TRY_CAST():
            return TryCast(expr=expr, data_type=output_type)
        raise ValueError(f"Unepxected cast expression {ctx.getText()}")

    @overrides
    def visitGenericType(
        self, ctx: SqlBaseParser.GenericTypeContext
    ) -> DataType:
        param_values = [self.visit(param) for param in ctx.typeParameter()]
        type_name = self.visit(ctx.identifier()).upper()
        # We assume the parameters will be passed into here.
        parameters = {
            param.name: val
            for val, param in zip(param_values, FIELDS[type_name])
        }
        return DataType(type_name, parameters=parameters)

    @overrides
    def visitRowType(self, ctx: SqlBaseParser.RowTypeContext) -> DataType:
        types = [self.visit(row) for row in ctx.rowField()]
        return row(dtypes=types)

    @overrides
    def visitRowField(self, ctx: SqlBaseParser.RowFieldContext) -> DataType:
        assert (
            ctx.identifier() is None
        ), "Data types with identifiers currently not supported."
        return self.visit(ctx.type_())

    @overrides
    def visitInterval(self, ctx: SqlBaseParser.IntervalContext) -> Interval:
        string_value = self.visit(ctx.string())
        # Unary plus is essentially a no-op.
        if string_value.startswith("+"):
            string_value = string_value.lstrip("+")

        # There's a pesky sign that can appear outside of the string. We do some inference on the string
        # and push the negation inside the string. I've verified that -"3-100" YEAR TO MONTH is equivalent to "-3-100".
        if ctx.sign and ctx.sign.text == "-":
            # I've tested this and the unary operator +/- can only be applied once to the values inside the string.
            # This means the string can either be +value, -value, or value.
            if string_value.startswith("-"):
                string_value = string_value.lstrip("-")
            else:
                string_value = "-" + string_value
        parameters = {
            "value": string_value,
            "from_interval": self.visit(ctx.from_),
        }
        if ctx.to is not None:
            parameters["to_interval"] = self.visit(ctx.to)
        return Interval(**parameters)

    @overrides
    def visitIntervalField(
        self, ctx: SqlBaseParser.IntervalFieldContext
    ) -> str:
        return ctx.getText()

    @overrides
    def visitIntervalType(
        self, ctx: SqlBaseParser.IntervalTypeContext
    ) -> DataType:
        parameters = {"from_interval": self.visit(ctx.from_)}
        if ctx.to:
            parameters["to_interval"] = self.visit(ctx.to)
        return interval(**parameters)

    @overrides
    def visitArrayType(self, ctx: SqlBaseParser.ArrayTypeContext) -> DataType:
        assert not ctx.INTEGER_VALUE(), "Explicit array size not supported"
        return array(dtype=self.visit(ctx.type_()))

    @overrides
    def visitTypeParameter(
        self, ctx: SqlBaseParser.TypeParameterContext
    ) -> Union[int, DataType]:
        if ctx.type_():
            return self.visit(ctx.type_())
        else:
            assert ctx.INTEGER_VALUE()
            return int(ctx.INTEGER_VALUE().getText())

    @overrides
    def visitDateTimeType(
        self, ctx: SqlBaseParser.DateTimeTypeContext
    ) -> DataType:
        parameters = {}
        data_type = TIMESTAMP if ctx.TIMESTAMP() else TIME

        if ctx.precision:
            parameters["precision"] = self.visit(ctx.precision)
        if ctx.WITH() and ctx.TIME() and ctx.ZONE():
            parameters["timezone"] = True
        else:
            parameters["timezone"] = False
        return DataType(data_type, parameters=parameters)

    @overrides
    def visitDoublePrecisionType(
        self, ctx: SqlBaseParser.DoublePrecisionTypeContext
    ) -> DataType:
        return double()

    @overrides
    def visitLegacyArrayType(
        self, ctx: SqlBaseParser.LegacyArrayTypeContext
    ) -> DataType:
        raise NotImplementedError("Legacy array type not yet supported")

    @overrides
    def visitLegacyMapType(
        self, ctx: SqlBaseParser.LegacyMapTypeContext
    ) -> DataType:
        raise NotImplementedError("Legacy map type not yet supported")

    @overrides
    def visitDereference(self, ctx: SqlBaseParser.DereferenceContext) -> Field:
        primary_expr = self.visit(ctx.primaryExpression())
        if isinstance(primary_expr, Field):
            table = primary_expr.name
            assert (
                not primary_expr.table
            ), "Treeno currently only supports dereference in the form x or y.x"
        else:
            table = primary_expr
        return Field(self.visit(ctx.fieldName), table)

    @overrides
    def visitSpecialDateTimeFunction(
        self, ctx: SqlBaseParser.SpecialDateTimeFunctionContext
    ) -> Function:
        fn_name = ctx.name.text.upper()
        assert (
            fn_name in NAMES_TO_FUNCTIONS
        ), f"Function name {fn_name} not registered in treeno.functions"
        fn = NAMES_TO_FUNCTIONS[fn_name]
        args = []
        if ctx.precision:
            args.append(int(ctx.precision.text))
        return fn(*args)

    @overrides
    def visitCurrentUser(
        self, ctx: SqlBaseParser.CurrentUserContext
    ) -> CurrentUser:
        return CurrentUser()

    @overrides
    def visitCurrentCatalog(
        self, ctx: SqlBaseParser.CurrentCatalogContext
    ) -> CurrentCatalog:
        return CurrentCatalog()

    @overrides
    def visitCurrentSchema(
        self, ctx: SqlBaseParser.CurrentSchemaContext
    ) -> CurrentSchema:
        return CurrentSchema()

    @overrides
    def visitCurrentPath(
        self, ctx: SqlBaseParser.CurrentPathContext
    ) -> CurrentPath:
        return CurrentPath()

    @overrides
    def visitNullLiteral(
        self, ctx: SqlBaseParser.NullLiteralContext
    ) -> Literal:
        # A null literal doesn't have a well-defined type, since it can be any type
        # due to all types in Trino being optional.
        return Literal(None, data_type=unknown())

    @overrides
    def visitNumericLiteral(
        self, ctx: SqlBaseParser.NumericLiteralContext
    ) -> Literal:
        # IMPORTANT: Note that number is only used in primary expression, so it's okay
        # that we get a literal from here. However, if number changes to support other
        # semantic meaning that's not a Literal value, then we need to change this.
        return self.visit(ctx.number())

    @overrides
    def visitIntegerLiteral(
        self, ctx: SqlBaseParser.IntegerLiteralContext
    ) -> Literal:
        value = int(ctx.INTEGER_VALUE().getText())
        if ctx.MINUS() is not None:
            value = -value
        return Literal(value, data_type=infer_integral(value))

    @overrides
    def visitDecimalLiteral(
        self, ctx: SqlBaseParser.DecimalLiteralContext
    ) -> Literal:
        text = ctx.DECIMAL_VALUE().getText()
        negative = ctx.MINUS() is not None

        dtype: DataType
        value: Union[int, Decimal]
        if "." in text:
            value = Decimal(text)
            dtype = infer_decimal(value)
        else:
            value = int(text)
            dtype = integer()

        if negative:
            value = -value

        return Literal(value, data_type=dtype)

    @overrides
    def visitStringLiteral(
        self, ctx: SqlBaseParser.StringLiteralContext
    ) -> Literal:
        string = self.visit(ctx.string())
        return Literal(string, data_type=varchar(max_chars=len(string)))

    @overrides
    def visitDoubleLiteral(
        self, ctx: SqlBaseParser.DoubleLiteralContext
    ) -> Literal:
        number_string = ctx.DOUBLE_VALUE().getText()
        if ctx.MINUS() is not None:
            number_string = ctx.MINUS().getText() + number_string
        return Literal(float(number_string), data_type=double())

    @overrides
    def visitBasicStringLiteral(
        self, ctx: SqlBaseParser.BasicStringLiteralContext
    ) -> str:
        return ctx.getText().strip("'")

    @overrides
    def visitUnicodeStringLiteral(
        self, ctx: SqlBaseParser.UnicodeStringLiteralContext
    ) -> str:
        if ctx.UESCAPE():
            assert ctx.STRING(), "Escape string must be supplied for unicode"
            escape_seq = ctx.STRING().getText().strip("'")
        else:
            escape_seq = "\\"
        s = (
            ctx.UNICODE_STRING()
            .getText()
            .strip("U&")
            .strip("'")
            .replace(escape_seq, "\\u")
            .encode("raw_unicode_escape")
            .decode("unicode_escape")
        )
        return s

    @overrides
    def visitBooleanLiteral(
        self, ctx: SqlBaseParser.BooleanLiteralContext
    ) -> Literal:
        return Literal(self.visit(ctx.booleanValue()), data_type=boolean())

    @overrides
    def visitBooleanValue(self, ctx: SqlBaseParser.BooleanValueContext) -> bool:
        return ctx.TRUE() is not None

    @overrides
    def visitTypeConstructor(
        self, ctx: SqlBaseParser.TypeConstructorContext
    ) -> TypeConstructor:
        value = self.visit(ctx.string())
        if ctx.DOUBLE() and ctx.PRECISION():
            return TypeConstructor(value, data_type=double())
        # It appears the type constructor is fairly primitive in that it doesn't allow parametrized types, like
        # SELECT DECIMAL(30) '3'
        # which means we can assume it's a generic(but inferred) nonparametrized data type.
        type_name = self.visit(ctx.identifier())
        return TypeConstructor(value=value, type_name=type_name)

    @overrides
    def visitFunctionCall(
        self, ctx: SqlBaseParser.FunctionCallContext
    ) -> Function:
        assert (
            not ctx.processingMode()
        ), "Pattern recognition is currently not supported"

        # Qualified names usually have multiple parts, but afaik functions aren't namespaced so there should only
        # be one part
        qual_name = self.visit(ctx.qualifiedName())
        assert (
            len(qual_name) == 1
        ), f"Invalid function name {'.'.join(qual_name)}"

        fn_name = qual_name[0].upper()
        assert (
            fn_name in NAMES_TO_FUNCTIONS
        ), f"Function name {fn_name} not registered in treeno.functions"
        fn = NAMES_TO_FUNCTIONS[fn_name]

        kwargs = {}

        if ctx.over():
            assert issubclass(
                fn, AggregateFunction
            ), "Can't scan over windows on non-aggregate functions"
            kwargs["window"] = self.visit(ctx.over())

        sort_items = ctx.sortItem()
        if sort_items:
            kwargs["orderby"] = [
                self.visit(sort_item) for sort_item in sort_items
            ]

        filter_ = ctx.filter_()
        if filter_:
            kwargs["filter_"] = self.visit(filter_)

        null_treatment = ctx.nullTreatment()
        if null_treatment:
            kwargs["null_treatment"] = self.visit(null_treatment)

        expressions: List[Value]
        if ctx.ASTERISK():
            star: Star
            if ctx.label:
                # If this is an identifier, treat it as an aliased relation
                label = self.visit(ctx.label)
                table = label.name if isinstance(label, Field) else label
                star = Star(table=table)
            else:
                star = Star()
            expressions = [star]
        else:
            # TODO: Are we missing the empty args case?
            expressions = [self.visit(expr) for expr in ctx.expression()]

        return fn.from_args(*expressions, **kwargs)

    @overrides
    def visitLambda_(self, ctx: SqlBaseParser.Lambda_Context) -> Lambda:
        # TODO: We have to implement better tree traversal to get this to work.
        variables = [
            Lambda.Variable(self.visit(identifier))
            for identifier in ctx.identifier()
        ]
        return Lambda.from_generic_expr(variables, self.visit(ctx.expression()))

    @overrides
    def visitNullTreatment(
        self, ctx: SqlBaseParser.NullTreatmentContext
    ) -> NullTreatment:
        if ctx.IGNORE():
            return NullTreatment.IGNORE
        elif ctx.RESPECT():
            return NullTreatment.RESPECT
        else:
            raise ValueError(f"Null treatment {ctx.getText()} not supported")

    @overrides
    def visitFilter_(self, ctx: SqlBaseParser.Filter_Context) -> Value:
        # For now, filter_ is only used inside of a query, solely to get the boolean value.
        return self.visit(ctx.booleanExpression())

    @overrides
    def visitRowConstructor(
        self, ctx: SqlBaseParser.RowConstructorContext
    ) -> RowConstructor:
        values = [self.visit(expr) for expr in ctx.expression()]
        return RowConstructor(values)

    @overrides
    def visitListagg(self, ctx: SqlBaseParser.ListaggContext) -> ListAgg:
        kwargs = {}
        separator = ctx.string()
        if separator:
            kwargs["separator"] = self.visit(separator)

        overflow_behavior = ctx.listAggOverflowBehavior()
        if overflow_behavior:
            kwargs["overflow_filler"] = self.visit(overflow_behavior)

        sort_items = ctx.sortItem()
        if sort_items:
            kwargs["orderby"] = [self.visit(item) for item in sort_items]
        return ListAgg(self.visit(ctx.expression()), **kwargs)

    @overrides
    def visitListAggOverflowBehavior(
        self, ctx: SqlBaseParser.ListAggOverflowBehaviorContext
    ) -> Optional[OverflowFiller]:
        # This is by default None in the parameter. It's only used in LISTAGG.
        if ctx.ERROR():
            return None
        filler = (
            self.visit(ctx.string())
            if ctx.string()
            else OverflowFiller.DEFAULT_FILLER
        )
        return OverflowFiller(
            count_indication=self.visit(ctx.listaggCountIndication()),
            filler=filler,
        )

    @overrides
    def visitListaggCountIndication(
        self, ctx: SqlBaseParser.ListaggCountIndicationContext
    ) -> CountIndication:
        if ctx.WITHOUT():
            return CountIndication.WITHOUT_COUNT
        elif ctx.WITH():
            return CountIndication.WITH_COUNT
        raise ValueError(f"Unknown count indication mode {ctx.getText()}")

    @overrides
    def visitParameter(self, ctx: SqlBaseParser.ParameterContext) -> Literal:
        raise NotImplementedError("Parameters currently not supported")

    @overrides
    def visitOver(self, ctx: SqlBaseParser.OverContext) -> Window:
        """The window can either be an identifier or a full window specification.
        """
        if ctx.windowName:
            return Window(parent_window=self.visit(ctx.windowName))
        return self.visit(ctx.windowSpecification())

    @overrides
    def visitArrayConstructor(
        self, ctx: SqlBaseParser.ArrayConstructorContext
    ) -> Array:
        return Array([self.visit(expr) for expr in ctx.expression()])

    @overrides
    def visitSubscript(self, ctx: SqlBaseParser.SubscriptContext) -> Subscript:
        return Subscript(
            value=self.visit(ctx.value), index=self.visit(ctx.index)
        )

    @overrides
    def visitValueExpressionDefault(
        self, ctx: SqlBaseParser.ValueExpressionDefaultContext
    ) -> Value:
        return self.visit(ctx.primaryExpression())

    @overrides
    def visitSelectSingle(
        self, ctx: SqlBaseParser.SelectSingleContext
    ) -> Value:
        value = self.visit(ctx.expression())
        identifier = ctx.identifier()
        if identifier:
            alias = self.visit(identifier)
            return AliasedValue(value, alias)
        return value

    @overrides
    def visitQuotedIdentifier(
        self, ctx: SqlBaseParser.QuotedIdentifierContext
    ) -> str:
        return ctx.getText().strip('"')

    @overrides
    def visitUnquotedIdentifier(
        self, ctx: SqlBaseParser.UnquotedIdentifierContext
    ) -> str:
        return ctx.getText()

    @overrides
    def visitBackQuotedIdentifier(
        self, ctx: SqlBaseParser.BackQuotedIdentifierContext
    ) -> str:
        raise NotImplementedError("Trino doesn't support backticks AFAIK")

    @overrides
    def visitDigitIdentifier(self, ctx: SqlBaseParser.DigitIdentifierContext):
        raise NotImplementedError(
            "Trino doesn't support digit identifiers AFAIK"
        )

    @overrides
    def visitColumnReference(
        self, ctx: SqlBaseParser.ColumnReferenceContext
    ) -> Field:
        # A column reference can be one of many forms of identifiers
        identifier = ctx.identifier()
        return Field(self.visit(identifier))

    @overrides
    def visitSubqueryExpression(
        self, ctx: SqlBaseParser.SubqueryExpressionContext
    ) -> SelectQuery:
        return self.visit(ctx.query())

    @overrides
    def visitParenthesizedExpression(
        self, ctx: SqlBaseParser.ParenthesizedExpressionContext
    ) -> Value:
        return self.visit(ctx.expression())

    @overrides
    def visitSelectAll(self, ctx: SqlBaseParser.SelectAllContext) -> Value:
        """Visits a `*` or `"table".*` statement. Returns a Star field.
        """
        if ctx.getText() == "*":
            return Star()
        primary_expr = ctx.primaryExpression()
        table: Optional[Union[str, Value]] = None
        if primary_expr:
            expr = self.visit(primary_expr)
            table = expr.name if isinstance(expr, Field) else expr

        # TODO: This is a bit hacky right now, but the table reference is derived from a ColumnReference which is a field
        # here we just interpret a Field to mean a table reference.
        column_aliases = ctx.columnAliases()
        star = Star(table)
        if column_aliases:
            return AliasedStar(star, self.visit(column_aliases))
        return star

    @overrides
    def visitJoinRelation(self, ctx: SqlBaseParser.JoinRelationContext) -> Join:
        left_relation = self.visit(ctx.left)
        # This part of the Trino grammar is really weird - why make it complicated and have `rightRelation` for
        # most join types but make it `right` for cross join and natural joins?
        right_relation = (
            self.visit(ctx.right)
            if ctx.right
            else self.visit(ctx.rightRelation)
        )
        config: JoinConfig
        if ctx.CROSS() and ctx.JOIN():
            config = JoinConfig(JoinType.CROSS)
        elif ctx.NATURAL():
            config = JoinConfig(
                join_type=self.visit(ctx.joinType()), natural=True
            )
        else:
            config = JoinConfig(
                join_type=self.visit(ctx.joinType()),
                criteria=self.visit(ctx.joinCriteria()),
            )
        return Join(left_relation, right_relation, config=config)

    @overrides
    def visitJoinCriteria(
        self, ctx: SqlBaseParser.JoinCriteriaContext
    ) -> JoinCriteria:
        if ctx.USING():
            return JoinUsingCriteria(
                column_names=[
                    self.visit(identifier) for identifier in ctx.identifier()
                ]
            )
        return JoinOnCriteria(constraint=self.visit(ctx.booleanExpression()))

    @overrides
    def visitJoinType(self, ctx: SqlBaseParser.JoinTypeContext) -> JoinType:
        if ctx.LEFT():
            return JoinType.LEFT
        if ctx.RIGHT():
            return JoinType.RIGHT
        if ctx.FULL():
            return JoinType.OUTER
        return JoinType.INNER

    @overrides
    def visitRelationDefault(
        self, ctx: SqlBaseParser.RelationDefaultContext
    ) -> Relation:
        return self.visit(ctx.sampledRelation())

    @overrides
    def visitSampledRelation(
        self, ctx: SqlBaseParser.SampledRelationContext
    ) -> Relation:
        relation = self.visit(ctx.patternRecognition())
        if ctx.TABLESAMPLE():
            return TableSample(
                relation,
                self.visit(ctx.sampleType()),
                self.visit(ctx.percentage),
            )
        return relation

    @overrides
    def visitSampleType(
        self, ctx: SqlBaseParser.SampleTypeContext
    ) -> SampleType:
        return SampleType[ctx.getText()]

    @overrides
    def visitPatternRecognition(
        self, ctx: SqlBaseParser.PatternRecognitionContext
    ) -> Relation:
        relation = self.visit(ctx.aliasedRelation())
        # TODO: Implement MATCH RECOGNIZE
        assert not ctx.MATCH_RECOGNIZE(), "Match recognizes are not supported"
        return relation

    @overrides
    def visitColumnAliases(
        self, ctx: SqlBaseParser.ColumnAliasesContext
    ) -> List[str]:
        return [self.visit(identifier) for identifier in ctx.identifier()]

    @overrides
    def visitQualifiedName(
        self, ctx: SqlBaseParser.QualifiedNameContext
    ) -> List[str]:
        return [self.visit(identifier) for identifier in ctx.identifier()]

    @overrides
    def visitTableName(self, ctx: SqlBaseParser.TableNameContext) -> Table:
        assert not ctx.queryPeriod(), "Query period not supported"
        qualifiers = self.visit(ctx.qualifiedName())
        return table_from_qualifiers(qualifiers)

    @overrides
    def visitSubqueryRelation(
        self, ctx: SqlBaseParser.SubqueryRelationContext
    ) -> SelectQuery:
        return self.visit(ctx.query())

    @overrides
    def visitParenthesizedRelation(
        self, ctx: SqlBaseParser.ParenthesizedRelationContext
    ) -> Relation:
        return self.visit(ctx.relation())

    @overrides
    def visitUnnest(self, ctx: SqlBaseParser.UnnestContext) -> Unnest:
        array_values = [self.visit(expr) for expr in ctx.expression()]
        with_ordinality = ctx.ORDINALITY() is not None
        return Unnest(arrays=array_values, with_ordinality=with_ordinality)

    @overrides
    def visitLateral(self, ctx: SqlBaseParser.LateralContext) -> Lateral:
        return Lateral(subquery=self.visit(ctx.query()))

    @overrides
    def visitAliasedRelation(
        self, ctx: SqlBaseParser.AliasedRelationContext
    ) -> Relation:
        relation = self.visit(ctx.relationPrimary())

        identifier = ctx.identifier()
        if not identifier:
            return relation

        alias = AliasedRelation(relation, self.visit(identifier))
        column_aliases = ctx.columnAliases()
        if column_aliases:
            alias.column_aliases = self.visit(column_aliases)

        return alias

    @overrides
    def visitGroupBy(self, ctx: SqlBaseParser.GroupByContext) -> GroupBy:
        kwargs: Dict[str, Any] = {
            "groups": [self.visit(group) for group in ctx.groupingElement()]
        }
        quantifier = ctx.setQuantifier()
        if quantifier:
            kwargs["groupby_quantifier"] = self.visit(quantifier)
        return GroupBy(**kwargs)

    @overrides
    def visitSingleGroupingSet(
        self, ctx: SqlBaseParser.SingleGroupingSetContext
    ) -> GroupingSet:
        return self.visit(ctx.groupingSet())

    @overrides
    def visitGroupingSet(
        self, ctx: SqlBaseParser.GroupingSetContext
    ) -> GroupingSet:
        return GroupingSet([self.visit(expr) for expr in ctx.expression()])

    @overrides
    def visitRollup(self, ctx: SqlBaseParser.RollupContext) -> Rollup:
        return Rollup([self.visit(expr) for expr in ctx.expression()])

    @overrides
    def visitCube(self, ctx: SqlBaseParser.CubeContext) -> Cube:
        return Cube([self.visit(expr) for expr in ctx.expression()])

    @overrides
    def visitMultipleGroupingSets(
        self, ctx: SqlBaseParser.MultipleGroupingSetsContext
    ) -> GroupingSetList:
        return GroupingSetList(
            groups=[
                self.visit(grouping_set) for grouping_set in ctx.groupingSet()
            ]
        )

    @overrides
    def visitWindowDefinition(
        self, ctx: SqlBaseParser.WindowDefinitionContext
    ) -> Tuple[str, Window]:
        return (self.visit(ctx.name), self.visit(ctx.windowSpecification()))

    @overrides
    def visitWindowSpecification(
        self, ctx: SqlBaseParser.WindowSpecificationContext
    ) -> Window:
        window_frame = ctx.windowFrame()
        # Set to default Window type if not specified
        window = self.visit(window_frame) if window_frame else Window()
        if ctx.ORDER() and ctx.BY():
            window.orderby = [
                self.visit(sort_item) for sort_item in ctx.sortItem()
            ]
        if ctx.partition:
            window.partitions = [
                self.visit(partition) for partition in ctx.partition
            ]
        if ctx.existingWindowName:
            window.parent_window = self.visit(ctx.existingWindowName)
        return window

    @overrides
    def visitWindowFrame(self, ctx: SqlBaseParser.WindowFrameContext) -> Window:
        assert not ctx.measureDefinition(), "MEASURES currently not supported"
        assert not ctx.skipTo(), "AFTER MATCH currently not supported"
        assert (
            not ctx.INITIAL() and not ctx.SEEK()
        ), "INITIAL/SEEK currently not supported"
        assert not ctx.rowPattern(), "Pattern matching currently not supported"
        assert not ctx.subsetDefinition(), "SUBSET currently not supported"
        assert (
            not ctx.variableDefinition()
        ), "Variable definition in window currently not supported"
        window = self.visit(ctx.frameExtent())
        return window

    @overrides
    def visitFrameExtent(self, ctx: SqlBaseParser.FrameExtentContext) -> Window:
        params = {
            "frame_type": FrameType[ctx.frameType.text.upper()],
            "start_bound": self.visit(ctx.start),
        }
        if ctx.end:
            params["end_bound"] = self.visit(ctx.end)
        return Window(**params)

    @overrides
    def visitBoundedFrame(
        self, ctx: SqlBaseParser.BoundedFrameContext
    ) -> BoundedFrameBound:
        return BoundedFrameBound(
            bound_type=BoundType[ctx.boundType.text.upper()],
            offset=self.visit(ctx.expression()),
        )

    @overrides
    def visitUnboundedFrame(
        self, ctx: SqlBaseParser.UnboundedFrameContext
    ) -> UnboundedFrameBound:
        return UnboundedFrameBound(
            bound_type=BoundType[ctx.boundType.text.upper()]
        )

    @overrides
    def visitCurrentRowBound(
        self, ctx: SqlBaseParser.CurrentRowBoundContext
    ) -> CurrentFrameBound:
        return CurrentFrameBound()

    @overrides
    def visitQuerySpecification(
        self, ctx: SqlBaseParser.QuerySpecificationContext
    ) -> SelectQuery:
        # Always returns a list of items to select from
        select_terms = ctx.selectItem()
        query_builder = SelectQuery(
            select=[self.visit(item) for item in select_terms]
        )

        relations = ctx.relation()
        if relations:
            # TODO: These must be CROSS JOIN'ed with each other.
            # currently we don't support JOINs
            if len(relations) > 1:
                raise NotImplementedError(
                    "Currently multiple FROM relations are not supported"
                )
            query_builder.from_ = self.visit(relations[0])

        # Dictates whether we select ALL rows or DISTINCT rows (all by default)
        set_qualifier = ctx.setQuantifier()
        if set_qualifier:
            query_builder.select_quantifier = self.visit(set_qualifier)

        if ctx.where:
            query_builder.where = self.visit(ctx.where)
        if ctx.having:
            query_builder.having = self.visit(ctx.having)
        groupby = ctx.groupBy()
        if groupby:
            query_builder.groupby = self.visit(groupby)
        if ctx.WINDOW():
            query_builder.window = dict(
                self.visit(window_def) for window_def in ctx.windowDefinition()
            )
        return query_builder

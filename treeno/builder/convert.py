"""
Converts from our grammar into a buildable query tree.
"""
from overrides import overrides
from decimal import Decimal
from typing import Optional, List, Union, Tuple, Dict
from treeno.grammar.gen.SqlBaseVisitor import SqlBaseVisitor
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.expression import (
    Value,
    Array,
    And,
    Or,
    Between,
    Field,
    Star,
    AliasedStar,
    AliasedValue,
    InList,
    Interval,
    Like,
    IsNull,
    DistinctFrom,
    Literal,
    Subscript,
    TypeConstructor,
    TryCast,
    Cast,
    RowConstructor,
)
from treeno.relation import (
    Relation,
    AliasedRelation,
    Table,
    TableQuery,
    ValuesQuery,
    Query,
    Unnest,
    Lateral,
    Join,
    JoinType,
    JoinConfig,
    JoinOnCriteria,
    JoinUsingCriteria,
    JoinCriteria,
    SelectQuery,
    SetQuantifier,
)
from treeno.functions import Function, NAMES_TO_FUNCTIONS, AggregateFunction
from treeno.datatypes.types import FIELDS, DataType, TIMESTAMP, TIME
from treeno.datatypes.inference import infer_integral, infer_decimal
from treeno.groupby import GroupBy, GroupingSet, GroupingSetList, Cube, Rollup
from treeno.orderby import OrderTerm, OrderType, NullOrder
from treeno.window import (
    Window,
    BoundType,
    UnboundedFrameBound,
    BoundedFrameBound,
    CurrentFrameBound,
    FrameType,
)
from treeno.datatypes.builder import (
    double,
    unknown,
    boolean,
    row,
    varchar,
    interval,
    array,
    integer,
)
from treeno.util import nth
from treeno.grammar.parse import AST


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
            query.with_queries = named_queries
        return query

    @overrides
    def visitWith_(self, ctx: SqlBaseParser.With_Context) -> Dict[str, Query]:
        assert (
            not ctx.RECURSIVE()
        ), "Recursive with queries currently not supported"
        return dict(self.visit(named_query) for named_query in ctx.namedQuery())

    @overrides
    def visitNamedQuery(
        self, ctx: SqlBaseParser.NamedQueryContext
    ) -> Tuple[str, Query]:
        assert (
            not ctx.columnAliases()
        ), "Column aliases currently not supported in WITH clause"
        return (self.visit(ctx.name), self.visit(ctx.query()))

    @overrides
    def visitQueryNoWith(self, ctx: SqlBaseParser.QueryNoWithContext) -> Query:
        query_term = ctx.queryTerm()
        if not isinstance(query_term, SqlBaseParser.QueryTermDefaultContext):
            raise NotImplementedError("Set operations are not yet implemented")
        query = self.visit(query_term)
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
            if not ctx.ordering or ctx.ordering.text == "ASC"
            else OrderType.DESC
        )
        null_order = (
            NullOrder.LAST
            if not ctx.nullOrdering or ctx.nullOrdering.text == "LAST"
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
    def visitInSubquery(self, ctx: SqlBaseParser.InSubqueryContext) -> None:
        raise NotImplementedError(
            "In subquery boolean predicate is not yet supported"
        )

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
        return apply_operator(ctx.operator.text, left, right)

    @overrides
    def visitArithmeticUnary(
        self, ctx: SqlBaseParser.ArithmeticUnaryContext
    ) -> Value:
        return apply_operator(
            ctx.operator.text, self.visit(ctx.valueExpression())
        )

    @overrides
    def visitCast(self, ctx: SqlBaseParser.CastContext) -> Union[Cast, TryCast]:
        expr = self.visit(ctx.expression())
        output_type = self.visit(ctx.type_())
        if ctx.CAST():
            return Cast(expr=expr, type=output_type)
        if ctx.TRY_CAST():
            return TryCast(expr=expr, type=output_type)

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
        primary_expr = ctx.primaryExpression()
        assert isinstance(
            primary_expr, SqlBaseParser.ColumnReferenceContext
        ), "We can only dereference `table`.`column` for now."
        return Field(self.visit(ctx.fieldName), self.visit(primary_expr))

    @overrides
    def visitNullLiteral(
        self, ctx: SqlBaseParser.NullLiteralContext
    ) -> Literal:
        # A null literal doesn't have a well-defined type, since it can be any type
        # due to all types in Trino being optional.
        return Literal(None, unknown())

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
        return Literal(value, infer_integral(value))

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

        return Literal(value, dtype)

    @overrides
    def visitStringLiteral(
        self, ctx: SqlBaseParser.StringLiteralContext
    ) -> Literal:
        string = self.visit(ctx.string())
        return Literal(string, varchar(max_chars=len(string)))

    @overrides
    def visitDoubleLiteral(
        self, ctx: SqlBaseParser.DoubleLiteralContext
    ) -> Literal:
        number_string = ctx.DOUBLE_VALUE().getText()
        if ctx.MINUS() is not None:
            number_string = ctx.MINUS().getText() + number_string
        return Literal(float(number_string), double())

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
            escape_seq = ctx.STRING().getText()
        else:
            escape_seq = "\\"
        return ctx.getText().strip("U&").strip("'").replace(escape_seq, "\\u")

    @overrides
    def visitBooleanLiteral(
        self, ctx: SqlBaseParser.BooleanLiteralContext
    ) -> Literal:
        return Literal(self.visit(ctx.booleanValue()), boolean())

    @overrides
    def visitBooleanValue(self, ctx: SqlBaseParser.BooleanValueContext) -> bool:
        return ctx.TRUE() is not None

    @overrides
    def visitTypeConstructor(
        self, ctx: SqlBaseParser.TypeConstructorContext
    ) -> Literal:
        if ctx.DOUBLE() and ctx.PRECISION():
            return TypeConstructor(self.visit(ctx.string(), type=double()))
        # It appears the type constructor is fairly primitive in that it doesn't allow parametrized types, like
        # SELECT DECIMAL(30) '3'
        # which means we can assume it's a generic nonparametrized data type.
        return TypeConstructor(
            self.visit(ctx.string()),
            type=DataType(self.visit(ctx.identifier())),
        )

    @overrides
    def visitFunctionCall(
        self, ctx: SqlBaseParser.FunctionCallContext
    ) -> Function:
        assert (
            not ctx.processingMode()
        ), "Pattern recognition is currently not supported"
        assert not ctx.filter_(), "Filter is currently not supported"
        assert (
            not ctx.nullTreatment()
        ), "Null treatment is currently not supported"
        assert (
            not ctx.sortItem()
        ), "ORDER BY in expression is currently not supported"

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

        expressions: List[Value]
        if ctx.ASTERISK():
            expressions = [Star()]
        else:
            # TODO: Are we missing the empty args case?
            expressions = [self.visit(expr) for expr in ctx.expression()]
        return fn(*expressions, **kwargs)

    @overrides
    def visitRowConstructor(
        self, ctx: SqlBaseParser.RowConstructorContext
    ) -> RowConstructor:
        values = [self.visit(expr) for expr in ctx.expression()]
        return RowConstructor(values)

    @overrides
    def visitParameter(self, ctx: SqlBaseParser.ParameterContext) -> Literal:
        raise NotImplementedError("Parameters currently not supported")

    @overrides
    def visitOver(self, ctx: SqlBaseParser.OverContext) -> Window:
        """The window can either be an identifier or a full window specification.
        """
        if ctx.windowName:
            return Window(parent_window=ctx.visit(ctx.windowName))
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
        primary_expr = ctx.primaryExpression()
        value_or_field = self.visit(primary_expr)
        # When we visit primary expressions, we don't always get Literals or Values, we might get a string
        # reference to a column, in which case we return a Field. It can be confusing when to wrap a string
        # in a Literal or to interpret it as a field.
        if isinstance(primary_expr, SqlBaseParser.ColumnReferenceContext):
            assert isinstance(value_or_field, str)
            return Field(value_or_field)
        if value_or_field is None:
            import pdb

            pdb.set_trace()
        assert isinstance(value_or_field, Value)
        return value_or_field

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
    ) -> str:
        # A column reference can be one of many forms of identifiers
        identifier = ctx.identifier()
        return self.visit(identifier)

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
        # This could be a table reference. When we're joining multiple tables together,
        # it makes sense to select all columns from a given input table, in which case
        # this expression is defined.
        # NOTE: The class we're dealing with here says it's a ColumnReference, but in this case
        #       it's wrong and we're dealing with tables here.
        primary_expr = ctx.primaryExpression()
        table: Optional[str] = None
        if primary_expr:
            if not isinstance(
                primary_expr, SqlBaseParser.ColumnReferenceContext
            ):
                raise NotImplementedError(
                    "Only column references are supported for asterisk references"
                )
            table = self.visit(primary_expr)
        column_aliases = ctx.columnAliases()
        if column_aliases:
            return AliasedStar(table, self.visit(column_aliases))
        return Star(table)

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
        return JoinOnCriteria(relation=self.visit(ctx.booleanExpression()))

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
        # TODO: Implement table sample
        table_sample = ctx.sampleType()
        assert not table_sample, "Tample samples are not supported"
        return relation

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
        return Unnest(array=array_values, with_ordinality=with_ordinality)

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
        kwargs = {
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
            "frame_type": FrameType[ctx.frameType.text],
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
            bound_type=BoundType[ctx.boundType.text],
            offset=self.visit(ctx.expression()),
        )

    @overrides
    def visitUnboundedFrame(
        self, ctx: SqlBaseParser.UnboundedFrameContext
    ) -> UnboundedFrameBound:
        return UnboundedFrameBound(bound_type=BoundType[ctx.boundType.text])

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
            query_builder.from_relation = self.visit(relations[0])

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

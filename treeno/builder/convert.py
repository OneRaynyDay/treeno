"""
Converts from our grammar into a buildable query tree.
"""
import attr
from typing import Optional, List, Union
from enum import Enum, auto
from treeno.grammar.gen.SqlBaseVisitor import SqlBaseVisitor
from treeno.grammar.gen.SqlBaseParser import SqlBaseParser
from treeno.expression import (
    Value,
    Between,
    Field,
    Star,
    AliasedValue,
    InList,
    Like,
    IsNull,
    DistinctFrom,
    Literal,
    Expression,
    TryCast,
    Cast,
)
from treeno.relation import (
    Relation,
    AliasedRelation,
    Table,
    Unnest,
    Lateral,
    Join,
    JoinType,
    JoinConfig,
    JoinOnCriteria,
    JoinUsingCriteria,
    JoinCriteria,
)
from treeno.types import (
    FIELDS,
    DataType,
    TIMESTAMP,
    INTERVAL,
    TIME,
    DOUBLE,
    ROW,
    ARRAY,
)
from treeno.util import nth


class SetQuantifier(Enum):
    DISTINCT = auto()
    ALL = auto()


@attr.s
class QueryBuilder(Relation):
    """Represents a high level SELECT query.
    A QueryBuilder can be converted to a SubqueryExpression if we are attempting to reinterpret the query as a set of
    ROW types.
    """

    select_values: List[Value] = attr.ib()
    from_relation: Optional[Relation] = attr.ib(default=None)
    where_value: Optional[Value] = attr.ib(default=None)
    groupby_values: Optional[List[Value]] = attr.ib(default=None)
    orderby_values: Optional[List[Value]] = attr.ib(default=None)
    having_value: Optional[Value] = attr.ib(default=None)
    select_quantifier: SetQuantifier = attr.ib(default=SetQuantifier.ALL)
    window: Optional[Value] = attr.ib(default=None)
    offset: Optional[Value] = attr.ib(default=None)
    limit: Optional[int] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        assert not self.orderby_values, "Orderby isn't supported"
        assert not self.offset, "Offset isn't supported"
        assert not self.window, "Window isn't supported"

    def __str__(self) -> str:
        str_builder = ["SELECT"]
        # All is the default, so we don't need to mention it
        if self.select_quantifier != SetQuantifier.ALL:
            str_builder.append(self.select_quantifier.name)

        str_builder.append(",".join(str(val) for val in self.select_values))
        if self.from_relation:
            str_builder += ["FROM", str(self.from_relation)]
        if self.where_value:
            str_builder += ["WHERE", str(self.where_value)]
        if self.groupby_values:
            str_builder += [
                "GROUP BY",
                ",".join(str(val) for val in self.groupby_values),
            ]
        if self.having_value:
            str_builder += ["HAVING", str(self.having_value)]
        if self.window:
            str_builder += ["WINDOW", str(self.window)]
        if self.orderby_values:
            str_builder += [
                "ORDER BY ",
                ",".join(str(order) for order in self.orderby_values),
            ]
        if self.offset:
            str_builder += ["OFFSET", str(self.offset)]
        if self.limit:
            str_builder += ["LIMIT", str(self.limit)]
        return " ".join(str_builder)


@attr.s
class SubqueryExpression(Expression):
    """In order to select a subquery expression directly, the query itself must be surrounded in parentheses.
    This class should be used as a lightweight wrapper over a prebuilt querybuilder.
    """

    query: QueryBuilder = attr.ib()

    def __str__(self) -> str:
        return f"({self.query})"


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


class ConvertVisitor(SqlBaseVisitor):
    """Converts the tree into a builder tree in python
    """

    def visitSingleStatement(self, ctx: SqlBaseParser.SingleStatementContext):
        stmt = ctx.statement()
        if not isinstance(stmt, SqlBaseParser.StatementDefaultContext):
            raise NotImplementedError(
                "Only standard selects are implemented for now"
            )
        return self.visit(ctx.statement())

    def visitStatementDefault(self, ctx: SqlBaseParser.StatementDefaultContext):
        return self.visit(ctx.query())

    def visitQuery(self, ctx: SqlBaseParser.QueryContext):
        if ctx.with_():
            raise NotImplementedError("With statements are not yet implemented")
        return self.visit(ctx.queryNoWith())

    def visitQueryNoWith(
        self, ctx: SqlBaseParser.QueryNoWithContext
    ) -> QueryBuilder:
        query_term = ctx.queryTerm()
        if not isinstance(query_term, SqlBaseParser.QueryTermDefaultContext):
            raise NotImplementedError("Set operations are not yet implemented")
        query = self.visit(query_term)
        limit_clause = ctx.limitRowCount()
        if limit_clause:
            # TODO: Assign this to the query object
            query.limit = self.visit(limit_clause)
        return query

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

    def visitQueryTermDefault(
        self, ctx: SqlBaseParser.QueryTermDefaultContext
    ) -> QueryBuilder:
        return self.visit(ctx.queryPrimary())

    def visitQueryPrimaryDefault(
        self, ctx: SqlBaseParser.QueryPrimaryDefaultContext
    ) -> QueryBuilder:
        query_spec = ctx.querySpecification()
        if not isinstance(query_spec, SqlBaseParser.QuerySpecificationContext):
            raise NotImplementedError(
                "Directly referencing tables without a SELECT clause is not yet implemented"
            )
        return self.visit(query_spec)

    def visitSetQuantifier(
        self, ctx: SqlBaseParser.SetQuantifierContext
    ) -> SetQuantifier:
        return SetQuantifier[ctx.getText()]

    def visitLogicalBinary(
        self, ctx: SqlBaseParser.LogicalBinaryContext
    ) -> Value:
        left = self.visit(ctx.left)
        right = self.visit(ctx.right)
        operator = ctx.operator.text
        return apply_operator(operator, left, right)

    def visitLogicalNot(self, ctx: SqlBaseParser.LogicalNotContext) -> Value:
        return ~self.visit(ctx.booleanExpression())

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

    def visitQuantifiedComparison(
        self, ctx: SqlBaseParser.QuantifiedComparisonContext
    ) -> None:
        raise NotImplementedError("Quantified comparison is not yet supported")

    def visitInSubquery(self, ctx: SqlBaseParser.InSubqueryContext) -> None:
        raise NotImplementedError(
            "In subquery boolean predicate is not yet supported"
        )

    def visitBetween(self, ctx: SqlBaseParser.BetweenContext) -> Value:
        between = Between(
            ctx.left_value,
            lower=self.visit(ctx.lower),
            upper=self.visit(ctx.upper),
        )
        if ctx.NOT():
            return ~between
        return between

    def visitInList(self, ctx: SqlBaseParser.InListContext) -> Value:
        expressions = ctx.expression()
        in_list = InList(
            value=ctx.left_value,
            exprs=[self.visit(expr) for expr in expressions],
        )
        if ctx.NOT():
            return ~in_list
        return in_list

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

    def visitNullPredicate(
        self, ctx: SqlBaseParser.NullPredicateContext
    ) -> Value:
        is_null = IsNull(ctx.left_value)
        if ctx.NOT():
            return ~is_null
        return is_null

    def visitDistinctFrom(
        self, ctx: SqlBaseParser.DistinctFromContext
    ) -> Value:
        distinct = DistinctFrom(ctx.left_value, right=self.visit(ctx.right))
        if ctx.NOT():
            return ~distinct
        return distinct

    def visitArithmeticBinary(
        self, ctx: SqlBaseParser.ArithmeticBinaryContext
    ) -> Value:
        left = self.visit(ctx.left)
        right = self.visit(ctx.right)
        return apply_operator(ctx.operator.text, left, right)

    def visitArithmeticUnary(
        self, ctx: SqlBaseParser.ArithmeticUnaryContext
    ) -> Value:
        return apply_operator(
            ctx.operator.text, self.visit(ctx.valueExpression())
        )

    def visitCast(self, ctx: SqlBaseParser.CastContext) -> Union[Cast, TryCast]:
        expr = self.visit(ctx.expression())
        output_type = self.visit(ctx.type_())
        if ctx.CAST():
            return Cast(expr=expr, type=output_type)
        if ctx.TRY_CAST():
            return TryCast(expr=expr, type=output_type)

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

    def visitRowType(self, ctx: SqlBaseParser.RowTypeContext) -> DataType:
        types = [self.visit(row) for row in ctx.rowField()]
        return DataType(ROW, parameters={"dtypes": types})

    def visitRowField(self, ctx: SqlBaseParser.RowFieldContext) -> DataType:
        assert (
            ctx.identifier() is None
        ), "Data types with identifiers currently not supported."
        return self.visit(ctx.type_())

    def visitIntervalType(
        self, ctx: SqlBaseParser.IntervalTypeContext
    ) -> DataType:
        parameters = {"from_interval": self.visit(ctx.from_)}
        if ctx.to:
            parameters["to_interval"] = self.visit(ctx.to)
        return DataType(INTERVAL, parameters=parameters)

    def visitArrayType(self, ctx: SqlBaseParser.ArrayTypeContext) -> DataType:
        assert not ctx.INTEGER_VALUE(), "Explicit array size not supported"
        return DataType(ARRAY, parameters={"dtype": self.visit(ctx.type_())})

    def visitTypeParameter(
        self, ctx: SqlBaseParser.TypeParameterContext
    ) -> Union[int, DataType]:
        if ctx.type_():
            return self.visit(ctx.type_())
        else:
            assert ctx.INTEGER_VALUE()
            return int(ctx.INTEGER_VALUE().getText())

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

    def visitDoublePrecisionType(
        self, ctx: SqlBaseParser.DoublePrecisionTypeContext
    ) -> DataType:
        return DataType(DOUBLE)

    def visitLegacyArrayType(
        self, ctx: SqlBaseParser.LegacyArrayTypeContext
    ) -> DataType:
        raise NotImplementedError("Legacy array type not yet supported")

    def visitLegacyMapType(
        self, ctx: SqlBaseParser.LegacyMapTypeContext
    ) -> DataType:
        raise NotImplementedError("Legacy map type not yet supported")

    def visitDereference(self, ctx: SqlBaseParser.DereferenceContext) -> Field:
        primary_expr = ctx.primaryExpression()
        assert isinstance(
            primary_expr, SqlBaseParser.ColumnReferenceContext
        ), "We can only dereference `table`.`column` for now."
        return Field(self.visit(ctx.fieldName), self.visit(primary_expr))

    def visitNumericLiteral(
        self, ctx: SqlBaseParser.NumericLiteralContext
    ) -> Literal:
        return self.visit(ctx.number())

    def visitIntegerLiteral(
        self, ctx: SqlBaseParser.IntegerLiteralContext
    ) -> Literal:
        value = int(ctx.INTEGER_VALUE().getText())
        if ctx.MINUS() is not None:
            value = -value

        return Literal(value)

    def visitDecimalLiteral(
        self, ctx: SqlBaseParser.DecimalLiteralContext
    ) -> Literal:
        text = ctx.DECIMAL_VALUE().getText()
        negative = ctx.MINUS() is not None

        if "." in text:
            value = float(text)
        else:
            value = int(text)

        if negative:
            value = -value

        return Literal(value)

    def visitStringLiteral(
        self, ctx: SqlBaseParser.StringLiteralContext
    ) -> Literal:
        return Literal(self.visit(ctx.string()))

    def visitBasicStringLiteral(
        self, ctx: SqlBaseParser.BasicStringLiteralContext
    ) -> str:
        return ctx.getText().strip("'")

    def visitUnicodeStringLiteral(
        self, ctx: SqlBaseParser.UnicodeStringLiteralContext
    ) -> str:
        raise NotImplementedError("Unicode characters currently not supported")

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
        assert isinstance(value_or_field, Value)
        return value_or_field

    def visitDoubleLiteral(
        self, ctx: SqlBaseParser.DoubleLiteralContext
    ) -> Literal:
        value = float(ctx.DOUBLE_VALUE().text)
        if ctx.MINUS() is not None:
            value = -value
        return Literal(value)

    def visitSelectSingle(
        self, ctx: SqlBaseParser.SelectSingleContext
    ) -> Value:
        """Selects a single column and names it"""
        # Currently we only support column names in the primary expression
        value = self.visit(ctx.expression())
        identifier = ctx.identifier()
        if identifier:
            alias = self.visit(identifier)
            return AliasedValue(value, alias)
        return value

    def visitQuotedIdentifier(
        self, ctx: SqlBaseParser.QuotedIdentifierContext
    ) -> str:
        return ctx.getText().strip('"')

    def visitUnquotedIdentifier(
        self, ctx: SqlBaseParser.UnquotedIdentifierContext
    ) -> str:
        return ctx.getText()

    def visitBackQuotedIdentifier(
        self, ctx: SqlBaseParser.BackQuotedIdentifierContext
    ) -> str:
        raise NotImplementedError("Trino doesn't support backticks AFAIK")

    def visitDigitIdentifier(self, ctx: SqlBaseParser.DigitIdentifierContext):
        raise NotImplementedError(
            "Trino doesn't support digit identifiers AFAIK"
        )

    def visitColumnReference(
        self, ctx: SqlBaseParser.ColumnReferenceContext
    ) -> str:
        # A column reference can be one of many forms of identifiers
        identifier = ctx.identifier()
        return self.visit(identifier)

    def visitSubqueryExpression(
        self, ctx: SqlBaseParser.SubqueryExpressionContext
    ) -> SubqueryExpression:
        return SubqueryExpression(query=self.visit(ctx.query()))

    def visitParenthesizedExpression(
        self, ctx: SqlBaseParser.ParenthesizedExpressionContext
    ) -> Value:
        return self.visit(ctx.expression())

    def visitSelectAll(self, ctx: SqlBaseParser.SelectAllContext) -> Star:
        """Visits a `*` or `"table".*` statement. Returns a Star field.
        """
        star = Star()
        if ctx.getText() == "*":
            return star
        # This could be a table reference. When we're joining multiple tables together,
        # it makes sense to select all columns from a given input table, in which case
        # this expression is defined.
        # NOTE: The class we're dealing with here says it's a ColumnReference, but in this case
        #       it's wrong and we're dealing with tables here.
        primary_expr = ctx.primaryExpression()
        if primary_expr:
            if not isinstance(
                primary_expr, SqlBaseParser.ColumnReferenceContext
            ):
                raise NotImplementedError(
                    "Only column references are supported for asterisk references"
                )
            star.table = self.visit(primary_expr)
        assert not ctx.columnAliases(), "* aliases not supported"
        return star

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

    def visitJoinType(self, ctx: SqlBaseParser.JoinTypeContext) -> JoinType:
        if ctx.LEFT():
            return JoinType.LEFT
        if ctx.RIGHT():
            return JoinType.RIGHT
        if ctx.FULL():
            return JoinType.OUTER
        return JoinType.INNER

    def visitRelationDefault(
        self, ctx: SqlBaseParser.RelationDefaultContext
    ) -> Relation:
        return self.visit(ctx.sampledRelation())

    def visitSampledRelation(
        self, ctx: SqlBaseParser.SampledRelationContext
    ) -> Relation:
        relation = self.visit(ctx.patternRecognition())
        # TODO: Implement table sample
        table_sample = ctx.sampleType()
        assert not table_sample, "Tample samples are not supported"
        return relation

    def visitPatternRecognition(
        self, ctx: SqlBaseParser.PatternRecognitionContext
    ) -> Relation:
        relation = self.visit(ctx.aliasedRelation())
        # TODO: Implement MATCH RECOGNIZE
        assert not ctx.MATCH_RECOGNIZE(), "Match recognizes are not supported"
        return relation

    def visitColumnAliases(
        self, ctx: SqlBaseParser.ColumnAliasesContext
    ) -> List[str]:
        return [self.visit(identifier) for identifier in ctx.identifier()]

    def visitQualifiedName(
        self, ctx: SqlBaseParser.QualifiedNameContext
    ) -> List[str]:
        return [self.visit(identifier) for identifier in ctx.identifier()]

    def visitTableName(self, ctx: SqlBaseParser.TableNameContext) -> Table:
        qualifiers = self.visit(ctx.qualifiedName())
        qualifiers.reverse()

        name: str = qualifiers[0]
        schema: Optional[str] = nth(qualifiers, 1)
        catalog: Optional[str] = nth(qualifiers, 2)

        return Table(name=name, schema=schema, catalog=catalog)

    def visitSubqueryRelation(
        self, ctx: SqlBaseParser.SubqueryRelationContext
    ) -> QueryBuilder:
        return self.visit(ctx.query())

    def visitParenthesizedRelation(
        self, ctx: SqlBaseParser.ParenthesizedRelationContext
    ) -> Relation:
        return self.visit(ctx.relation())

    def visitUnnest(self, ctx: SqlBaseParser.UnnestContext) -> Unnest:
        raise NotImplementedError("Unnest currently not supported")

    def visitLateral(self, ctx: SqlBaseParser.LateralContext) -> Lateral:
        raise NotImplementedError("Lateral currently not supported")

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
            alias.column_aliases = [
                self.visit(column_alias) for column_alias in column_aliases
            ]

        return alias

    def visitQuerySpecification(
        self, ctx: SqlBaseParser.QuerySpecificationContext
    ) -> QueryBuilder:
        # Always returns a list of items to select from
        select_terms = ctx.selectItem()
        query_builder = QueryBuilder(
            select_values=[self.visit(item) for item in select_terms]
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
            query_builder.where_value = self.visit(ctx.where)
        if ctx.having:
            query_builder.having = self.visit(ctx.having)
        return query_builder

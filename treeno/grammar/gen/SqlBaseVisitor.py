# Generated from SqlBase.g4 by ANTLR 4.9.2
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .SqlBaseParser import SqlBaseParser
else:
    from SqlBaseParser import SqlBaseParser

# This class defines a complete generic visitor for a parse tree produced by SqlBaseParser.

class SqlBaseVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by SqlBaseParser#singleStatement.
    def visitSingleStatement(self, ctx:SqlBaseParser.SingleStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#standaloneExpression.
    def visitStandaloneExpression(self, ctx:SqlBaseParser.StandaloneExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#standalonePathSpecification.
    def visitStandalonePathSpecification(self, ctx:SqlBaseParser.StandalonePathSpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#standaloneType.
    def visitStandaloneType(self, ctx:SqlBaseParser.StandaloneTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#standaloneRowPattern.
    def visitStandaloneRowPattern(self, ctx:SqlBaseParser.StandaloneRowPatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#statementDefault.
    def visitStatementDefault(self, ctx:SqlBaseParser.StatementDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#use.
    def visitUse(self, ctx:SqlBaseParser.UseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createSchema.
    def visitCreateSchema(self, ctx:SqlBaseParser.CreateSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropSchema.
    def visitDropSchema(self, ctx:SqlBaseParser.DropSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameSchema.
    def visitRenameSchema(self, ctx:SqlBaseParser.RenameSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setSchemaAuthorization.
    def visitSetSchemaAuthorization(self, ctx:SqlBaseParser.SetSchemaAuthorizationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTableAsSelect.
    def visitCreateTableAsSelect(self, ctx:SqlBaseParser.CreateTableAsSelectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createTable.
    def visitCreateTable(self, ctx:SqlBaseParser.CreateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropTable.
    def visitDropTable(self, ctx:SqlBaseParser.DropTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#insertInto.
    def visitInsertInto(self, ctx:SqlBaseParser.InsertIntoContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#delete.
    def visitDelete(self, ctx:SqlBaseParser.DeleteContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameTable.
    def visitRenameTable(self, ctx:SqlBaseParser.RenameTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#commentTable.
    def visitCommentTable(self, ctx:SqlBaseParser.CommentTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#commentColumn.
    def visitCommentColumn(self, ctx:SqlBaseParser.CommentColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameColumn.
    def visitRenameColumn(self, ctx:SqlBaseParser.RenameColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropColumn.
    def visitDropColumn(self, ctx:SqlBaseParser.DropColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#addColumn.
    def visitAddColumn(self, ctx:SqlBaseParser.AddColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setTableAuthorization.
    def visitSetTableAuthorization(self, ctx:SqlBaseParser.SetTableAuthorizationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#analyze.
    def visitAnalyze(self, ctx:SqlBaseParser.AnalyzeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createMaterializedView.
    def visitCreateMaterializedView(self, ctx:SqlBaseParser.CreateMaterializedViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createView.
    def visitCreateView(self, ctx:SqlBaseParser.CreateViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#refreshMaterializedView.
    def visitRefreshMaterializedView(self, ctx:SqlBaseParser.RefreshMaterializedViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropMaterializedView.
    def visitDropMaterializedView(self, ctx:SqlBaseParser.DropMaterializedViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropView.
    def visitDropView(self, ctx:SqlBaseParser.DropViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#renameView.
    def visitRenameView(self, ctx:SqlBaseParser.RenameViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setViewAuthorization.
    def visitSetViewAuthorization(self, ctx:SqlBaseParser.SetViewAuthorizationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#call.
    def visitCall(self, ctx:SqlBaseParser.CallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#createRole.
    def visitCreateRole(self, ctx:SqlBaseParser.CreateRoleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dropRole.
    def visitDropRole(self, ctx:SqlBaseParser.DropRoleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#grantRoles.
    def visitGrantRoles(self, ctx:SqlBaseParser.GrantRolesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#revokeRoles.
    def visitRevokeRoles(self, ctx:SqlBaseParser.RevokeRolesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setRole.
    def visitSetRole(self, ctx:SqlBaseParser.SetRoleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#grant.
    def visitGrant(self, ctx:SqlBaseParser.GrantContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#revoke.
    def visitRevoke(self, ctx:SqlBaseParser.RevokeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showGrants.
    def visitShowGrants(self, ctx:SqlBaseParser.ShowGrantsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#explain.
    def visitExplain(self, ctx:SqlBaseParser.ExplainContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCreateTable.
    def visitShowCreateTable(self, ctx:SqlBaseParser.ShowCreateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCreateSchema.
    def visitShowCreateSchema(self, ctx:SqlBaseParser.ShowCreateSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCreateView.
    def visitShowCreateView(self, ctx:SqlBaseParser.ShowCreateViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCreateMaterializedView.
    def visitShowCreateMaterializedView(self, ctx:SqlBaseParser.ShowCreateMaterializedViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showTables.
    def visitShowTables(self, ctx:SqlBaseParser.ShowTablesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showSchemas.
    def visitShowSchemas(self, ctx:SqlBaseParser.ShowSchemasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showCatalogs.
    def visitShowCatalogs(self, ctx:SqlBaseParser.ShowCatalogsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showColumns.
    def visitShowColumns(self, ctx:SqlBaseParser.ShowColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showStats.
    def visitShowStats(self, ctx:SqlBaseParser.ShowStatsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showStatsForQuery.
    def visitShowStatsForQuery(self, ctx:SqlBaseParser.ShowStatsForQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showRoles.
    def visitShowRoles(self, ctx:SqlBaseParser.ShowRolesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showRoleGrants.
    def visitShowRoleGrants(self, ctx:SqlBaseParser.ShowRoleGrantsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showFunctions.
    def visitShowFunctions(self, ctx:SqlBaseParser.ShowFunctionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#showSession.
    def visitShowSession(self, ctx:SqlBaseParser.ShowSessionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setSession.
    def visitSetSession(self, ctx:SqlBaseParser.SetSessionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#resetSession.
    def visitResetSession(self, ctx:SqlBaseParser.ResetSessionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#startTransaction.
    def visitStartTransaction(self, ctx:SqlBaseParser.StartTransactionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#commit.
    def visitCommit(self, ctx:SqlBaseParser.CommitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rollback.
    def visitRollback(self, ctx:SqlBaseParser.RollbackContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#prepare.
    def visitPrepare(self, ctx:SqlBaseParser.PrepareContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#deallocate.
    def visitDeallocate(self, ctx:SqlBaseParser.DeallocateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#execute.
    def visitExecute(self, ctx:SqlBaseParser.ExecuteContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeInput.
    def visitDescribeInput(self, ctx:SqlBaseParser.DescribeInputContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#describeOutput.
    def visitDescribeOutput(self, ctx:SqlBaseParser.DescribeOutputContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setPath.
    def visitSetPath(self, ctx:SqlBaseParser.SetPathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#update.
    def visitUpdate(self, ctx:SqlBaseParser.UpdateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#merge.
    def visitMerge(self, ctx:SqlBaseParser.MergeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#query.
    def visitQuery(self, ctx:SqlBaseParser.QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#with_.
    def visitWith_(self, ctx:SqlBaseParser.With_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableElement.
    def visitTableElement(self, ctx:SqlBaseParser.TableElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#columnDefinition.
    def visitColumnDefinition(self, ctx:SqlBaseParser.ColumnDefinitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#likeClause.
    def visitLikeClause(self, ctx:SqlBaseParser.LikeClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#properties.
    def visitProperties(self, ctx:SqlBaseParser.PropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#property_.
    def visitProperty_(self, ctx:SqlBaseParser.Property_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#queryNoWith.
    def visitQueryNoWith(self, ctx:SqlBaseParser.QueryNoWithContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#limitRowCount.
    def visitLimitRowCount(self, ctx:SqlBaseParser.LimitRowCountContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowCount.
    def visitRowCount(self, ctx:SqlBaseParser.RowCountContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#queryTermDefault.
    def visitQueryTermDefault(self, ctx:SqlBaseParser.QueryTermDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setOperation.
    def visitSetOperation(self, ctx:SqlBaseParser.SetOperationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#queryPrimaryDefault.
    def visitQueryPrimaryDefault(self, ctx:SqlBaseParser.QueryPrimaryDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#table.
    def visitTable(self, ctx:SqlBaseParser.TableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#inlineTable.
    def visitInlineTable(self, ctx:SqlBaseParser.InlineTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subquery.
    def visitSubquery(self, ctx:SqlBaseParser.SubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sortItem.
    def visitSortItem(self, ctx:SqlBaseParser.SortItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#querySpecification.
    def visitQuerySpecification(self, ctx:SqlBaseParser.QuerySpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupBy.
    def visitGroupBy(self, ctx:SqlBaseParser.GroupByContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#singleGroupingSet.
    def visitSingleGroupingSet(self, ctx:SqlBaseParser.SingleGroupingSetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rollup.
    def visitRollup(self, ctx:SqlBaseParser.RollupContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#cube.
    def visitCube(self, ctx:SqlBaseParser.CubeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#multipleGroupingSets.
    def visitMultipleGroupingSets(self, ctx:SqlBaseParser.MultipleGroupingSetsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupingSet.
    def visitGroupingSet(self, ctx:SqlBaseParser.GroupingSetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowDefinition.
    def visitWindowDefinition(self, ctx:SqlBaseParser.WindowDefinitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowSpecification.
    def visitWindowSpecification(self, ctx:SqlBaseParser.WindowSpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namedQuery.
    def visitNamedQuery(self, ctx:SqlBaseParser.NamedQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#setQuantifier.
    def visitSetQuantifier(self, ctx:SqlBaseParser.SetQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#selectSingle.
    def visitSelectSingle(self, ctx:SqlBaseParser.SelectSingleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#selectAll.
    def visitSelectAll(self, ctx:SqlBaseParser.SelectAllContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#relationDefault.
    def visitRelationDefault(self, ctx:SqlBaseParser.RelationDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#joinRelation.
    def visitJoinRelation(self, ctx:SqlBaseParser.JoinRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#joinType.
    def visitJoinType(self, ctx:SqlBaseParser.JoinTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#joinCriteria.
    def visitJoinCriteria(self, ctx:SqlBaseParser.JoinCriteriaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sampledRelation.
    def visitSampledRelation(self, ctx:SqlBaseParser.SampledRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#sampleType.
    def visitSampleType(self, ctx:SqlBaseParser.SampleTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#patternRecognition.
    def visitPatternRecognition(self, ctx:SqlBaseParser.PatternRecognitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#measureDefinition.
    def visitMeasureDefinition(self, ctx:SqlBaseParser.MeasureDefinitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowsPerMatch.
    def visitRowsPerMatch(self, ctx:SqlBaseParser.RowsPerMatchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#emptyMatchHandling.
    def visitEmptyMatchHandling(self, ctx:SqlBaseParser.EmptyMatchHandlingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#skipTo.
    def visitSkipTo(self, ctx:SqlBaseParser.SkipToContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subsetDefinition.
    def visitSubsetDefinition(self, ctx:SqlBaseParser.SubsetDefinitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#variableDefinition.
    def visitVariableDefinition(self, ctx:SqlBaseParser.VariableDefinitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#aliasedRelation.
    def visitAliasedRelation(self, ctx:SqlBaseParser.AliasedRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#columnAliases.
    def visitColumnAliases(self, ctx:SqlBaseParser.ColumnAliasesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#tableName.
    def visitTableName(self, ctx:SqlBaseParser.TableNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subqueryRelation.
    def visitSubqueryRelation(self, ctx:SqlBaseParser.SubqueryRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unnest.
    def visitUnnest(self, ctx:SqlBaseParser.UnnestContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#lateral.
    def visitLateral(self, ctx:SqlBaseParser.LateralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#parenthesizedRelation.
    def visitParenthesizedRelation(self, ctx:SqlBaseParser.ParenthesizedRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#expression.
    def visitExpression(self, ctx:SqlBaseParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#logicalNot.
    def visitLogicalNot(self, ctx:SqlBaseParser.LogicalNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#predicated.
    def visitPredicated(self, ctx:SqlBaseParser.PredicatedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#logicalBinary.
    def visitLogicalBinary(self, ctx:SqlBaseParser.LogicalBinaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#comparison.
    def visitComparison(self, ctx:SqlBaseParser.ComparisonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#quantifiedComparison.
    def visitQuantifiedComparison(self, ctx:SqlBaseParser.QuantifiedComparisonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#between.
    def visitBetween(self, ctx:SqlBaseParser.BetweenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#inList.
    def visitInList(self, ctx:SqlBaseParser.InListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#inSubquery.
    def visitInSubquery(self, ctx:SqlBaseParser.InSubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#like.
    def visitLike(self, ctx:SqlBaseParser.LikeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nullPredicate.
    def visitNullPredicate(self, ctx:SqlBaseParser.NullPredicateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#distinctFrom.
    def visitDistinctFrom(self, ctx:SqlBaseParser.DistinctFromContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#valueExpressionDefault.
    def visitValueExpressionDefault(self, ctx:SqlBaseParser.ValueExpressionDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#concatenation.
    def visitConcatenation(self, ctx:SqlBaseParser.ConcatenationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arithmeticBinary.
    def visitArithmeticBinary(self, ctx:SqlBaseParser.ArithmeticBinaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arithmeticUnary.
    def visitArithmeticUnary(self, ctx:SqlBaseParser.ArithmeticUnaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#atTimeZone.
    def visitAtTimeZone(self, ctx:SqlBaseParser.AtTimeZoneContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dereference.
    def visitDereference(self, ctx:SqlBaseParser.DereferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#typeConstructor.
    def visitTypeConstructor(self, ctx:SqlBaseParser.TypeConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#specialDateTimeFunction.
    def visitSpecialDateTimeFunction(self, ctx:SqlBaseParser.SpecialDateTimeFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#substring.
    def visitSubstring(self, ctx:SqlBaseParser.SubstringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#cast.
    def visitCast(self, ctx:SqlBaseParser.CastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#lambda.
    def visitLambda(self, ctx:SqlBaseParser.LambdaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#parenthesizedExpression.
    def visitParenthesizedExpression(self, ctx:SqlBaseParser.ParenthesizedExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#parameter.
    def visitParameter(self, ctx:SqlBaseParser.ParameterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#normalize.
    def visitNormalize(self, ctx:SqlBaseParser.NormalizeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#intervalLiteral.
    def visitIntervalLiteral(self, ctx:SqlBaseParser.IntervalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#numericLiteral.
    def visitNumericLiteral(self, ctx:SqlBaseParser.NumericLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#booleanLiteral.
    def visitBooleanLiteral(self, ctx:SqlBaseParser.BooleanLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#simpleCase.
    def visitSimpleCase(self, ctx:SqlBaseParser.SimpleCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#columnReference.
    def visitColumnReference(self, ctx:SqlBaseParser.ColumnReferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nullLiteral.
    def visitNullLiteral(self, ctx:SqlBaseParser.NullLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowConstructor.
    def visitRowConstructor(self, ctx:SqlBaseParser.RowConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subscript.
    def visitSubscript(self, ctx:SqlBaseParser.SubscriptContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#currentPath.
    def visitCurrentPath(self, ctx:SqlBaseParser.CurrentPathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#subqueryExpression.
    def visitSubqueryExpression(self, ctx:SqlBaseParser.SubqueryExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#binaryLiteral.
    def visitBinaryLiteral(self, ctx:SqlBaseParser.BinaryLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#currentUser.
    def visitCurrentUser(self, ctx:SqlBaseParser.CurrentUserContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#extract.
    def visitExtract(self, ctx:SqlBaseParser.ExtractContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#stringLiteral.
    def visitStringLiteral(self, ctx:SqlBaseParser.StringLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arrayConstructor.
    def visitArrayConstructor(self, ctx:SqlBaseParser.ArrayConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#functionCall.
    def visitFunctionCall(self, ctx:SqlBaseParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#exists.
    def visitExists(self, ctx:SqlBaseParser.ExistsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#position.
    def visitPosition(self, ctx:SqlBaseParser.PositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#searchedCase.
    def visitSearchedCase(self, ctx:SqlBaseParser.SearchedCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupingOperation.
    def visitGroupingOperation(self, ctx:SqlBaseParser.GroupingOperationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#processingMode.
    def visitProcessingMode(self, ctx:SqlBaseParser.ProcessingModeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nullTreatment.
    def visitNullTreatment(self, ctx:SqlBaseParser.NullTreatmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#basicStringLiteral.
    def visitBasicStringLiteral(self, ctx:SqlBaseParser.BasicStringLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unicodeStringLiteral.
    def visitUnicodeStringLiteral(self, ctx:SqlBaseParser.UnicodeStringLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#timeZoneInterval.
    def visitTimeZoneInterval(self, ctx:SqlBaseParser.TimeZoneIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#timeZoneString.
    def visitTimeZoneString(self, ctx:SqlBaseParser.TimeZoneStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:SqlBaseParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#comparisonQuantifier.
    def visitComparisonQuantifier(self, ctx:SqlBaseParser.ComparisonQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#booleanValue.
    def visitBooleanValue(self, ctx:SqlBaseParser.BooleanValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#interval.
    def visitInterval(self, ctx:SqlBaseParser.IntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#intervalField.
    def visitIntervalField(self, ctx:SqlBaseParser.IntervalFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#normalForm.
    def visitNormalForm(self, ctx:SqlBaseParser.NormalFormContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowType.
    def visitRowType(self, ctx:SqlBaseParser.RowTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#intervalType.
    def visitIntervalType(self, ctx:SqlBaseParser.IntervalTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#arrayType.
    def visitArrayType(self, ctx:SqlBaseParser.ArrayTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#doublePrecisionType.
    def visitDoublePrecisionType(self, ctx:SqlBaseParser.DoublePrecisionTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#legacyArrayType.
    def visitLegacyArrayType(self, ctx:SqlBaseParser.LegacyArrayTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#genericType.
    def visitGenericType(self, ctx:SqlBaseParser.GenericTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#dateTimeType.
    def visitDateTimeType(self, ctx:SqlBaseParser.DateTimeTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#legacyMapType.
    def visitLegacyMapType(self, ctx:SqlBaseParser.LegacyMapTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rowField.
    def visitRowField(self, ctx:SqlBaseParser.RowFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#typeParameter.
    def visitTypeParameter(self, ctx:SqlBaseParser.TypeParameterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#whenClause.
    def visitWhenClause(self, ctx:SqlBaseParser.WhenClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#filter_.
    def visitFilter_(self, ctx:SqlBaseParser.Filter_Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#mergeUpdate.
    def visitMergeUpdate(self, ctx:SqlBaseParser.MergeUpdateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#mergeDelete.
    def visitMergeDelete(self, ctx:SqlBaseParser.MergeDeleteContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#mergeInsert.
    def visitMergeInsert(self, ctx:SqlBaseParser.MergeInsertContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#over.
    def visitOver(self, ctx:SqlBaseParser.OverContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#windowFrame.
    def visitWindowFrame(self, ctx:SqlBaseParser.WindowFrameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unboundedFrame.
    def visitUnboundedFrame(self, ctx:SqlBaseParser.UnboundedFrameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#currentRowBound.
    def visitCurrentRowBound(self, ctx:SqlBaseParser.CurrentRowBoundContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#boundedFrame.
    def visitBoundedFrame(self, ctx:SqlBaseParser.BoundedFrameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#quantifiedPrimary.
    def visitQuantifiedPrimary(self, ctx:SqlBaseParser.QuantifiedPrimaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#patternConcatenation.
    def visitPatternConcatenation(self, ctx:SqlBaseParser.PatternConcatenationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#patternAlternation.
    def visitPatternAlternation(self, ctx:SqlBaseParser.PatternAlternationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#patternVariable.
    def visitPatternVariable(self, ctx:SqlBaseParser.PatternVariableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#emptyPattern.
    def visitEmptyPattern(self, ctx:SqlBaseParser.EmptyPatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#patternPermutation.
    def visitPatternPermutation(self, ctx:SqlBaseParser.PatternPermutationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#groupedPattern.
    def visitGroupedPattern(self, ctx:SqlBaseParser.GroupedPatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionStartAnchor.
    def visitPartitionStartAnchor(self, ctx:SqlBaseParser.PartitionStartAnchorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#partitionEndAnchor.
    def visitPartitionEndAnchor(self, ctx:SqlBaseParser.PartitionEndAnchorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#excludedPattern.
    def visitExcludedPattern(self, ctx:SqlBaseParser.ExcludedPatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#zeroOrMoreQuantifier.
    def visitZeroOrMoreQuantifier(self, ctx:SqlBaseParser.ZeroOrMoreQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#oneOrMoreQuantifier.
    def visitOneOrMoreQuantifier(self, ctx:SqlBaseParser.OneOrMoreQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#zeroOrOneQuantifier.
    def visitZeroOrOneQuantifier(self, ctx:SqlBaseParser.ZeroOrOneQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rangeQuantifier.
    def visitRangeQuantifier(self, ctx:SqlBaseParser.RangeQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#updateAssignment.
    def visitUpdateAssignment(self, ctx:SqlBaseParser.UpdateAssignmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#explainFormat.
    def visitExplainFormat(self, ctx:SqlBaseParser.ExplainFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#explainType.
    def visitExplainType(self, ctx:SqlBaseParser.ExplainTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#isolationLevel.
    def visitIsolationLevel(self, ctx:SqlBaseParser.IsolationLevelContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#transactionAccessMode.
    def visitTransactionAccessMode(self, ctx:SqlBaseParser.TransactionAccessModeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#readUncommitted.
    def visitReadUncommitted(self, ctx:SqlBaseParser.ReadUncommittedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#readCommitted.
    def visitReadCommitted(self, ctx:SqlBaseParser.ReadCommittedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#repeatableRead.
    def visitRepeatableRead(self, ctx:SqlBaseParser.RepeatableReadContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#serializable.
    def visitSerializable(self, ctx:SqlBaseParser.SerializableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#positionalArgument.
    def visitPositionalArgument(self, ctx:SqlBaseParser.PositionalArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#namedArgument.
    def visitNamedArgument(self, ctx:SqlBaseParser.NamedArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#qualifiedArgument.
    def visitQualifiedArgument(self, ctx:SqlBaseParser.QualifiedArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unqualifiedArgument.
    def visitUnqualifiedArgument(self, ctx:SqlBaseParser.UnqualifiedArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#pathSpecification.
    def visitPathSpecification(self, ctx:SqlBaseParser.PathSpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#privilege.
    def visitPrivilege(self, ctx:SqlBaseParser.PrivilegeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#qualifiedName.
    def visitQualifiedName(self, ctx:SqlBaseParser.QualifiedNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#specifiedPrincipal.
    def visitSpecifiedPrincipal(self, ctx:SqlBaseParser.SpecifiedPrincipalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#currentUserGrantor.
    def visitCurrentUserGrantor(self, ctx:SqlBaseParser.CurrentUserGrantorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#currentRoleGrantor.
    def visitCurrentRoleGrantor(self, ctx:SqlBaseParser.CurrentRoleGrantorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unspecifiedPrincipal.
    def visitUnspecifiedPrincipal(self, ctx:SqlBaseParser.UnspecifiedPrincipalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#userPrincipal.
    def visitUserPrincipal(self, ctx:SqlBaseParser.UserPrincipalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#rolePrincipal.
    def visitRolePrincipal(self, ctx:SqlBaseParser.RolePrincipalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#roles.
    def visitRoles(self, ctx:SqlBaseParser.RolesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#unquotedIdentifier.
    def visitUnquotedIdentifier(self, ctx:SqlBaseParser.UnquotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#quotedIdentifier.
    def visitQuotedIdentifier(self, ctx:SqlBaseParser.QuotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#backQuotedIdentifier.
    def visitBackQuotedIdentifier(self, ctx:SqlBaseParser.BackQuotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#digitIdentifier.
    def visitDigitIdentifier(self, ctx:SqlBaseParser.DigitIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#decimalLiteral.
    def visitDecimalLiteral(self, ctx:SqlBaseParser.DecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#doubleLiteral.
    def visitDoubleLiteral(self, ctx:SqlBaseParser.DoubleLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#integerLiteral.
    def visitIntegerLiteral(self, ctx:SqlBaseParser.IntegerLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by SqlBaseParser#nonReserved.
    def visitNonReserved(self, ctx:SqlBaseParser.NonReservedContext):
        return self.visitChildren(ctx)



del SqlBaseParser
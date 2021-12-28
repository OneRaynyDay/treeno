/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar SqlBase;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

standaloneExpression
    : expression EOF
    ;

standalonePathSpecification
    : pathSpecification EOF
    ;

standaloneType
    : type_ EOF
    ;

standaloneRowPattern
    : rowPattern EOF
    ;

statement
    : query                                                            #statementDefault
    | USE schema=identifier                                            #use
    | USE catalog=identifier '.' schema=identifier                     #use
    | CREATE SCHEMA (IF NOT EXISTS)? qualifiedName
        (AUTHORIZATION principal)?
        (WITH properties)?                                             #createSchema
    | DROP SCHEMA (IF EXISTS)? qualifiedName (CASCADE | RESTRICT)?     #dropSchema
    | ALTER SCHEMA qualifiedName RENAME TO identifier                  #renameSchema
    | ALTER SCHEMA qualifiedName SET AUTHORIZATION principal           #setSchemaAuthorization
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName columnAliases?
        (COMMENT string)?
        (WITH properties)? AS (query | '('query')')
        (WITH (NO)? DATA)?                                             #createTableAsSelect
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
        '(' tableElement (',' tableElement)* ')'
         (COMMENT string)?
         (WITH properties)?                                            #createTable
    | DROP TABLE (IF EXISTS)? qualifiedName                            #dropTable
    | INSERT INTO qualifiedName columnAliases? query                   #insertInto
    | DELETE FROM qualifiedName (WHERE booleanExpression)?             #delete
    | TRUNCATE TABLE qualifiedName                                     #truncateTable
    | COMMENT ON TABLE qualifiedName IS (string | NULL)                #commentTable
    | COMMENT ON COLUMN qualifiedName IS (string | NULL)               #commentColumn
    | ALTER TABLE (IF EXISTS)? from_=qualifiedName
        RENAME TO to=qualifiedName                                     #renameTable
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ADD COLUMN (IF NOT EXISTS)? column=columnDefinition            #addColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        RENAME COLUMN (IF EXISTS)? from_=identifier TO to=identifier    #renameColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        DROP COLUMN (IF EXISTS)? column=qualifiedName                  #dropColumn
    | ALTER TABLE tableName=qualifiedName SET AUTHORIZATION principal  #setTableAuthorization
    | ALTER TABLE tableName=qualifiedName
        SET PROPERTIES propertyAssignments                             #setTableProperties
    | ALTER TABLE tableName=qualifiedName
        EXECUTE procedureName=identifier
        ('(' (callArgument (',' callArgument)*)? ')')?
        (WHERE where=booleanExpression)?                               #tableExecute
    | ANALYZE qualifiedName (WITH properties)?                         #analyze
    | CREATE (OR REPLACE)? MATERIALIZED VIEW
        (IF NOT EXISTS)? qualifiedName
        (COMMENT string)?
        (WITH properties)? AS query                                    #createMaterializedView
    | CREATE (OR REPLACE)? VIEW qualifiedName
        (COMMENT string)?
        (SECURITY (DEFINER | INVOKER))? AS query                       #createView
    | REFRESH MATERIALIZED VIEW qualifiedName                          #refreshMaterializedView
    | DROP MATERIALIZED VIEW (IF EXISTS)? qualifiedName                #dropMaterializedView
    | ALTER MATERIALIZED VIEW (IF EXISTS)? from_=qualifiedName
        RENAME TO to=qualifiedName                                     #renameMaterializedView
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    | ALTER VIEW from_=qualifiedName RENAME TO to=qualifiedName         #renameView
    | ALTER VIEW from_=qualifiedName SET AUTHORIZATION principal        #setViewAuthorization
    | CALL qualifiedName '(' (callArgument (',' callArgument)*)? ')'   #call
    | CREATE ROLE name=identifier
        (WITH ADMIN grantor)?
        (IN catalog=identifier)?                                       #createRole
    | DROP ROLE name=identifier (IN catalog=identifier)?            #dropRole
    | GRANT
        roles
        TO principal (',' principal)*
        (WITH ADMIN OPTION)?
        (GRANTED BY grantor)?
        (IN catalog=identifier)?                                       #grantRoles
    | REVOKE
        (ADMIN OPTION FOR)?
        roles
        FROM principal (',' principal)*
        (GRANTED BY grantor)?
        (IN catalog=identifier)?                                       #revokeRoles
    | SET ROLE (ALL | NONE | role=identifier)
        (IN catalog=identifier)?                                       #setRole
    | GRANT
        (privilege (',' privilege)* | ALL PRIVILEGES)
        ON (SCHEMA | TABLE)? qualifiedName
        TO grantee=principal
        (WITH GRANT OPTION)?                                           #grant
    | DENY
        (privilege (',' privilege)* | ALL PRIVILEGES)
        ON (SCHEMA | TABLE)? qualifiedName
        TO grantee=principal                                           #deny
    | REVOKE
        (GRANT OPTION FOR)?
        (privilege (',' privilege)* | ALL PRIVILEGES)
        ON (SCHEMA | TABLE)? qualifiedName
        FROM grantee=principal                                         #revoke
    | SHOW GRANTS (ON TABLE? qualifiedName)?                           #showGrants
    | EXPLAIN ('(' explainOption (',' explainOption)* ')')? statement  #explain
    | EXPLAIN ANALYZE VERBOSE? statement                               #explainAnalyze
    | SHOW CREATE TABLE qualifiedName                                  #showCreateTable
    | SHOW CREATE SCHEMA qualifiedName                                 #showCreateSchema
    | SHOW CREATE VIEW qualifiedName                                   #showCreateView
    | SHOW CREATE MATERIALIZED VIEW qualifiedName                      #showCreateMaterializedView
    | SHOW TABLES ((FROM | IN) qualifiedName)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier)?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showSchemas
    | SHOW CATALOGS
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName?
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showColumns
    | SHOW STATS FOR qualifiedName                                     #showStats
    | SHOW STATS FOR '(' query ')'                                     #showStatsForQuery
    | SHOW CURRENT? ROLES ((FROM | IN) identifier)?                    #showRoles
    | SHOW ROLE GRANTS ((FROM | IN) identifier)?                       #showRoleGrants
    | DESCRIBE qualifiedName                                           #showColumns
    | DESC qualifiedName                                               #showColumns
    | SHOW FUNCTIONS
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showFunctions
    | SHOW SESSION
        (LIKE pattern=string (ESCAPE escape=string)?)?                 #showSession
    | SET SESSION qualifiedName EQ expression                          #setSession
    | RESET SESSION qualifiedName                                      #resetSession
    | START TRANSACTION (transactionMode (',' transactionMode)*)?      #startTransaction
    | COMMIT WORK?                                                     #commit
    | ROLLBACK WORK?                                                   #rollback
    | PREPARE identifier FROM statement                                #prepare
    | DEALLOCATE PREPARE identifier                                    #deallocate
    | EXECUTE identifier (USING expression (',' expression)*)?         #execute
    | DESCRIBE INPUT identifier                                        #describeInput
    | DESCRIBE OUTPUT identifier                                       #describeOutput
    | SET PATH pathSpecification                                       #setPath
    | SET TIME ZONE (LOCAL | expression)                               #setTimeZone
    | UPDATE qualifiedName
        SET updateAssignment (',' updateAssignment)*
        (WHERE where=booleanExpression)?                               #update
    | MERGE INTO qualifiedName (AS? identifier)?
        USING relation ON expression mergeCase+                        #merge
    ;

query
    :  with_? queryNoWith
    ;

with_
    : WITH RECURSIVE? namedQuery (',' namedQuery)*
    ;

tableElement
    : columnDefinition
    | likeClause
    ;

columnDefinition
    : identifier type_ (NOT NULL)? (COMMENT string)? (WITH properties)?
    ;

likeClause
    : LIKE qualifiedName (optionType=(INCLUDING | EXCLUDING) PROPERTIES)?
    ;

properties
    : '(' propertyAssignments ')'
    ;

propertyAssignments
    : property_ (',' property_)*
    ;

property_
    : identifier EQ expression
    ;

queryNoWith
    : queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (OFFSET offset=rowCount (ROW | ROWS)?)?
      ( (LIMIT limit=limitRowCount)
      | (FETCH (FIRST | NEXT) (fetchFirst=rowCount)? (ROW | ROWS) (ONLY | WITH TIES))
      )?
    ;

limitRowCount
    : ALL
    | rowCount
    ;

rowCount
    : INTEGER_VALUE
    | QUESTION_MARK
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | TABLE qualifiedName                  #table
    | VALUES expression (',' expression)*  #inlineTable
    | '(' queryNoWith ')'                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
      (WINDOW windowDefinition (',' windowDefinition)*)?
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP '(' (expression (',' expression)*)? ')'         #rollup
    | CUBE '(' (expression (',' expression)*)? ')'           #cube
    | GROUPING SETS '(' groupingSet (',' groupingSet)* ')'   #multipleGroupingSets
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

windowDefinition
    : name=identifier AS '(' windowSpecification ')'
    ;

windowSpecification
    : (existingWindowName=identifier)?
      (PARTITION BY partition+=expression (',' partition+=expression)*)?
      (ORDER BY sortItem (',' sortItem)*)?
      windowFrame?
    ;

namedQuery
    : name=identifier (columnAliases)? AS '(' query ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?                          #selectSingle
    | primaryExpression '.' ASTERISK (AS columnAliases)?    #selectAll
    | ASTERISK                                              #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=sampledRelation
      )                                                     #joinRelation
    | sampledRelation                                       #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sampledRelation
    : patternRecognition (
        TABLESAMPLE sampleType '(' percentage=expression ')'
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

listAggOverflowBehavior
    : ERROR
    | TRUNCATE string? listaggCountIndication
    ;

listaggCountIndication
    : WITH COUNT
    | WITHOUT COUNT
    ;

patternRecognition
    : aliasedRelation (
        MATCH_RECOGNIZE '('
          (PARTITION BY partition+=expression (',' partition+=expression)*)?
          (ORDER BY sortItem (',' sortItem)*)?
          (MEASURES measureDefinition (',' measureDefinition)*)?
          rowsPerMatch?
          (AFTER MATCH skipTo)?
          (INITIAL | SEEK)?
          PATTERN '(' rowPattern ')'
          (SUBSET subsetDefinition (',' subsetDefinition)*)?
          DEFINE variableDefinition (',' variableDefinition)*
        ')'
        (AS? identifier columnAliases?)?
      )?
    ;

measureDefinition
    : expression AS identifier
    ;

rowsPerMatch
    : ONE ROW PER MATCH
    | ALL ROWS PER MATCH emptyMatchHandling?
    ;

emptyMatchHandling
    : SHOW EMPTY MATCHES
    | OMIT EMPTY MATCHES
    | WITH UNMATCHED ROWS
    ;

skipTo
    : 'SKIP' TO NEXT ROW
    | 'SKIP' PAST LAST ROW
    | 'SKIP' TO FIRST identifier
    | 'SKIP' TO LAST identifier
    | 'SKIP' TO identifier
    ;

subsetDefinition
    : name=identifier EQ '(' union+=identifier (',' union+=identifier)* ')'
    ;

variableDefinition
    : identifier AS expression
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName queryPeriod?                                      #tableName
    | '(' query ')'                                                   #subqueryRelation
    | UNNEST '(' expression (',' expression)* ')' (WITH ORDINALITY)?  #unnest
    | LATERAL '(' query ')'                                           #lateral
    | '(' relation ')'                                                #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?  #predicated
    | NOT booleanExpression                             #logicalNot
    | booleanExpression AND booleanExpression           #and_
    | booleanExpression OR booleanExpression            #or_
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | QUESTION_MARK                                                                       #parameter
    | POSITION '(' valueExpression IN valueExpression ')'                                 #position
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | ROW '(' expression (',' expression)* ')'                                            #rowConstructor
    | name=LISTAGG '(' setQuantifier? expression (',' string)?
        (ON OVERFLOW listAggOverflowBehavior)? ')'
        (WITHIN GROUP '(' ORDER BY sortItem (',' sortItem)* ')')                          #listagg
    | processingMode? qualifiedName '(' (label=identifier '.')? ASTERISK ')'
        filter_? over?                                                                     #functionCall
    | processingMode? qualifiedName '(' (setQuantifier? expression (',' expression)*)?
        (ORDER BY sortItem (',' sortItem)*)? ')' filter_? (nullTreatment? over)?           #functionCall
    | identifier over                                                                     #measure
    | identifier '->' expression                                                          #lambda_
    | '(' (identifier (',' identifier)*)? ')' '->' expression                             #lambda_
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST '(' expression AS type_ ')'                                                     #cast
    | TRY_CAST '(' expression AS type_ ')'                                                 #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                       #arrayConstructor
    | value=primaryExpression '[' index=valueExpression ']'                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #specialDateTimeFunction
    | name=CURRENT_TIME ('(' precision=INTEGER_VALUE ')')?                                #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP ('(' precision=INTEGER_VALUE ')')?                           #specialDateTimeFunction
    | name=LOCALTIME ('(' precision=INTEGER_VALUE ')')?                                   #specialDateTimeFunction
    | name=LOCALTIMESTAMP ('(' precision=INTEGER_VALUE ')')?                              #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | name=CURRENT_CATALOG                                                                #currentCatalog
    | name=CURRENT_SCHEMA                                                                 #currentSchema
    | name=CURRENT_PATH                                                                   #currentPath
    | SUBSTRING '(' valueExpression FROM valueExpression (FOR valueExpression)? ')'       #substring
    | NORMALIZE '(' valueExpression (',' normalForm)? ')'                                 #normalize
    | EXTRACT '(' identifier FROM valueExpression ')'                                     #extract
    | '(' expression ')'                                                                  #parenthesizedExpression
    | GROUPING '(' (qualifiedName (',' qualifiedName)*)? ')'                              #groupingOperation
    ;

processingMode
    : RUNNING
    | FINAL
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE string    #timeZoneString
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from_=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

type_
    : ROW '(' rowField (',' rowField)* ')'                                         #rowType
    | INTERVAL from_=intervalField (TO to=intervalField)?                           #intervalType
    | base=TIMESTAMP ('(' precision = typeParameter ')')? (WITHOUT TIME ZONE)?     #dateTimeType
    | base=TIMESTAMP ('(' precision = typeParameter ')')? WITH TIME ZONE           #dateTimeType
    | base=TIME ('(' precision = typeParameter ')')? (WITHOUT TIME ZONE)?          #dateTimeType
    | base=TIME ('(' precision = typeParameter ')')? WITH TIME ZONE                #dateTimeType
    | DOUBLE PRECISION                                                             #doublePrecisionType
    | ARRAY '<' type_ '>'                                                           #legacyArrayType
    | MAP '<' keyType=type_ ',' valueType=type_ '>'                                  #legacyMapType
    | type_ ARRAY ('[' INTEGER_VALUE ']')?                                          #arrayType
    | identifier ('(' typeParameter (',' typeParameter)* ')')?                     #genericType
    ;

rowField
    : type_
    | identifier type_;

typeParameter
    : INTEGER_VALUE | type_
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter_
    : FILTER '(' WHERE booleanExpression ')'
    ;

mergeCase
    : WHEN MATCHED (AND condition=expression)? THEN
        UPDATE SET targets+=identifier EQ values+=expression
          (',' targets+=identifier EQ values+=expression)*                  #mergeUpdate
    | WHEN MATCHED (AND condition=expression)? THEN DELETE                  #mergeDelete
    | WHEN NOT MATCHED (AND condition=expression)? THEN
        INSERT ('(' targets+=identifier (',' targets+=identifier)* ')')?
        VALUES '(' values+=expression (',' values+=expression)* ')'         #mergeInsert
    ;

over
    : OVER (windowName=identifier | '(' windowSpecification ')')
    ;

windowFrame
    : (MEASURES measureDefinition (',' measureDefinition)*)?
      frameExtent
      (AFTER MATCH skipTo)?
      (INITIAL | SEEK)?
      (PATTERN '(' rowPattern ')')?
      (SUBSET subsetDefinition (',' subsetDefinition)*)?
      (DEFINE variableDefinition (',' variableDefinition)*)?
    ;

frameExtent
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=GROUPS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    | frameType=GROUPS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;

rowPattern
    : patternPrimary patternQuantifier?                 #quantifiedPrimary
    | rowPattern rowPattern                             #patternConcatenation
    | rowPattern '|' rowPattern                         #patternAlternation
    ;

patternPrimary
    : identifier                                        #patternVariable
    | '(' ')'                                           #emptyPattern
    | PERMUTE '(' rowPattern (',' rowPattern)* ')'      #patternPermutation
    | '(' rowPattern ')'                                #groupedPattern
    | '^'                                               #partitionStartAnchor
    | '$'                                               #partitionEndAnchor
    | '{-' rowPattern '-}'                              #excludedPattern
    ;

patternQuantifier
    : ASTERISK (reluctant=QUESTION_MARK)?                                                       #zeroOrMoreQuantifier
    | PLUS (reluctant=QUESTION_MARK)?                                                           #oneOrMoreQuantifier
    | QUESTION_MARK (reluctant=QUESTION_MARK)?                                                  #zeroOrOneQuantifier
    | '{' exactly=INTEGER_VALUE '}' (reluctant=QUESTION_MARK)?                                  #rangeQuantifier
    | '{' (atLeast=INTEGER_VALUE)? ',' (atMost=INTEGER_VALUE)? '}' (reluctant=QUESTION_MARK)?   #rangeQuantifier
    ;

updateAssignment
    : identifier EQ expression
    ;

explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)                 #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE | IO)    #explainType
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

pathElement
    : identifier '.' identifier     #qualifiedArgument
    | identifier                    #unqualifiedArgument
    ;

pathSpecification
    : pathElement (',' pathElement)*
    ;

privilege
    : CREATE | SELECT | DELETE | INSERT | UPDATE
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

queryPeriod
    : FOR rangeType AS OF end=valueExpression
    ;

rangeType
    : TIMESTAMP
    | VERSION
    ;

grantor
    : principal             #specifiedPrincipal
    | CURRENT_USER          #currentUserGrantor
    | CURRENT_ROLE          #currentRoleGrantor
    ;

principal
    : identifier            #unspecifiedPrincipal
    | USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    ;

roles
    : identifier (',' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | MINUS? INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ADD | ADMIN | AFTER | ALL | ANALYZE | ANY | ARRAY | ASC | AT | AUTHORIZATION
    | BERNOULLI
    | CALL | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | COUNT | CURRENT
    | DATA | DATE | DAY | DEFINE | DEFINER | DESC | DISTRIBUTED | DOUBLE
    | EMPTY | ERROR | EXCLUDING | EXPLAIN
    | FETCH | FILTER | FINAL | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GRANT | DENY | GRANTED | GRANTS | GRAPHVIZ | GROUPS
    | HOUR
    | IF | IGNORE | INCLUDING | INITIAL | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LAST | LATERAL | LEVEL | LIMIT | LOCAL | LOGICAL
    | MAP | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MEASURES | MERGE | MINUTE | MONTH
    | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW
    | PARTITION | PARTITIONS | PAST | PATH | PATTERN | PER | PERMUTE | POSITION | PRECEDING | PRECISION | PRIVILEGES | PROPERTIES
    | RANGE | READ | REFRESH | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS | RUNNING
    | SCHEMA | SCHEMAS | SECOND | SECURITY | SEEK | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SOME | START | STATS | SUBSET | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TIES | TIME | TIMESTAMP | TO | TRANSACTION | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | UNMATCHED | UPDATE | USE | USER
    | VALIDATE | VERBOSE | VERSION | VIEW
    | WINDOW | WITHIN | WITHOUT | WORK | WRITE
    | YEAR
    | ZONE
    ;

ADD: 'ADD' | 'add';
ADMIN: 'ADMIN' | 'admin';
AFTER: 'AFTER' | 'after';
ALL: 'ALL' | 'all';
ALTER: 'ALTER' | 'alter';
ANALYZE: 'ANALYZE' | 'analyze';
AND: 'AND' | 'and';
ANY: 'ANY' | 'any';
ARRAY: 'ARRAY' | 'array';
AS: 'AS' | 'as';
ASC: 'ASC' | 'asc';
AT: 'AT' | 'at';
AUTHORIZATION: 'AUTHORIZATION' | 'authorization';
BERNOULLI: 'BERNOULLI' | 'bernoulli';
BETWEEN: 'BETWEEN' | 'between';
BY: 'BY' | 'by';
CALL: 'CALL' | 'call';
CASCADE: 'CASCADE' | 'cascade';
CASE: 'CASE' | 'case';
CAST: 'CAST' | 'cast';
CATALOGS: 'CATALOGS' | 'catalogs';
COLUMN: 'COLUMN' | 'column';
COLUMNS: 'COLUMNS' | 'columns';
COMMENT: 'COMMENT' | 'comment';
COMMIT: 'COMMIT' | 'commit';
COMMITTED: 'COMMITTED' | 'committed';
CONSTRAINT: 'CONSTRAINT' | 'constraint';
COUNT: 'COUNT' | 'count';
CREATE: 'CREATE' | 'create';
CROSS: 'CROSS' | 'cross';
CUBE: 'CUBE' | 'cube';
CURRENT: 'CURRENT' | 'current';
CURRENT_CATALOG: 'CURRENT_CATALOG' | 'current_catalog';
CURRENT_DATE: 'CURRENT_DATE' | 'current_date';
CURRENT_PATH: 'CURRENT_PATH' | 'current_path';
CURRENT_ROLE: 'CURRENT_ROLE' | 'current_role';
CURRENT_SCHEMA: 'CURRENT_SCHEMA' | 'current_schema';
CURRENT_TIME: 'CURRENT_TIME' | 'current_time';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP' | 'current_timestamp';
CURRENT_USER: 'CURRENT_USER' | 'current_user';
DATA: 'DATA' | 'data';
DATE: 'DATE' | 'date';
DAY: 'DAY' | 'day';
DEALLOCATE: 'DEALLOCATE' | 'deallocate';
DEFINER: 'DEFINER' | 'definer';
DELETE: 'DELETE' | 'delete';
DENY: 'DENY' | 'deny';
DESC: 'DESC' | 'desc';
DESCRIBE: 'DESCRIBE' | 'describe';
DEFINE: 'DEFINE' | 'define';
DISTINCT: 'DISTINCT' | 'distinct';
DISTRIBUTED: 'DISTRIBUTED' | 'distributed';
DOUBLE: 'DOUBLE' | 'double';
DROP: 'DROP' | 'drop';
ELSE: 'ELSE' | 'else';
EMPTY: 'EMPTY' | 'empty';
END: 'END' | 'end';
ERROR: 'ERROR' | 'error';
ESCAPE: 'ESCAPE' | 'escape';
EXCEPT: 'EXCEPT' | 'except';
EXCLUDING: 'EXCLUDING' | 'excluding';
EXECUTE: 'EXECUTE' | 'execute';
EXISTS: 'EXISTS' | 'exists';
EXPLAIN: 'EXPLAIN' | 'explain';
EXTRACT: 'EXTRACT' | 'extract';
FALSE: 'FALSE' | 'false';
FETCH: 'FETCH' | 'fetch';
FILTER: 'FILTER' | 'filter';
FINAL: 'FINAL' | 'final';
FIRST: 'FIRST' | 'first';
FOLLOWING: 'FOLLOWING' | 'following';
FOR: 'FOR' | 'for';
FORMAT: 'FORMAT' | 'format';
FROM: 'FROM' | 'from';
FULL: 'FULL' | 'full';
FUNCTIONS: 'FUNCTIONS' | 'functions';
GRANT: 'GRANT' | 'grant';
GRANTED: 'GRANTED' | 'granted';
GRANTS: 'GRANTS' | 'grants';
GRAPHVIZ: 'GRAPHVIZ' | 'graphviz';
GROUP: 'GROUP' | 'group';
GROUPING: 'GROUPING' | 'grouping';
GROUPS: 'GROUPS' | 'groups';
HAVING: 'HAVING' | 'having';
HOUR: 'HOUR' | 'hour';
IF: 'IF' | 'if';
IGNORE: 'IGNORE' | 'ignore';
IN: 'IN' | 'in';
INCLUDING: 'INCLUDING' | 'including';
INITIAL: 'INITIAL' | 'initial';
INNER: 'INNER' | 'inner';
INPUT: 'INPUT' | 'input';
INSERT: 'INSERT' | 'insert';
INTERSECT: 'INTERSECT' | 'intersect';
INTERVAL: 'INTERVAL' | 'interval';
INTO: 'INTO' | 'into';
INVOKER: 'INVOKER' | 'invoker';
IO: 'IO' | 'io';
IS: 'IS' | 'is';
ISOLATION: 'ISOLATION' | 'isolation';
JOIN: 'JOIN' | 'join';
JSON: 'JSON' | 'json';
LAST: 'LAST' | 'last';
LATERAL: 'LATERAL' | 'lateral';
LEFT: 'LEFT' | 'left';
LEVEL: 'LEVEL' | 'level';
LIKE: 'LIKE' | 'like';
LIMIT: 'LIMIT' | 'limit';
LISTAGG: 'LISTAGG' | 'listagg';
LOCAL: 'LOCAL' | 'local';
LOCALTIME: 'LOCALTIME' | 'localtime';
LOCALTIMESTAMP: 'LOCALTIMESTAMP' | 'localtimestamp';
LOGICAL: 'LOGICAL' | 'logical';
MAP: 'MAP' | 'map';
MATCH: 'MATCH' | 'match';
MATCHED: 'MATCHED' | 'matched';
MATCHES: 'MATCHES' | 'matches';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE' | 'match_recognize';
MATERIALIZED: 'MATERIALIZED' | 'materialized';
MEASURES: 'MEASURES' | 'measures';
MERGE: 'MERGE' | 'merge';
MINUTE: 'MINUTE' | 'minute';
MONTH: 'MONTH' | 'month';
NATURAL: 'NATURAL' | 'natural';
NEXT: 'NEXT' | 'next';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO' | 'no';
NONE: 'NONE' | 'none';
NORMALIZE: 'NORMALIZE' | 'normalize';
NOT: 'NOT' | 'not';
NULL: 'NULL' | 'null';
NULLIF: 'NULLIF' | 'nullif';
NULLS: 'NULLS' | 'nulls';
OFFSET: 'OFFSET' | 'offset';
OMIT: 'OMIT' | 'omit';
OF: 'OF' | 'of';
ON: 'ON' | 'on';
ONE: 'ONE' | 'one';
ONLY: 'ONLY' | 'only';
OPTION: 'OPTION' | 'option';
OR: 'OR' | 'or';
ORDER: 'ORDER' | 'order';
ORDINALITY: 'ORDINALITY' | 'ordinality';
OUTER: 'OUTER' | 'outer';
OUTPUT: 'OUTPUT' | 'output';
OVER: 'OVER' | 'over';
OVERFLOW: 'OVERFLOW' | 'overflow';
PARTITION: 'PARTITION' | 'partition';
PARTITIONS: 'PARTITIONS' | 'partitions';
PAST: 'PAST' | 'past';
PATH: 'PATH' | 'path';
PATTERN: 'PATTERN' | 'pattern';
PER: 'PER' | 'per';
PERMUTE: 'PERMUTE' | 'permute';
POSITION: 'POSITION' | 'position';
PRECEDING: 'PRECEDING' | 'preceding';
PRECISION: 'PRECISION' | 'precision';
PREPARE: 'PREPARE' | 'prepare';
PRIVILEGES: 'PRIVILEGES' | 'privileges';
PROPERTIES: 'PROPERTIES' | 'properties';
RANGE: 'RANGE' | 'range';
READ: 'READ' | 'read';
RECURSIVE: 'RECURSIVE' | 'recursive';
REFRESH: 'REFRESH' | 'refresh';
RENAME: 'RENAME' | 'rename';
REPEATABLE: 'REPEATABLE' | 'repeatable';
REPLACE: 'REPLACE' | 'replace';
RESET: 'RESET' | 'reset';
RESPECT: 'RESPECT' | 'respect';
RESTRICT: 'RESTRICT' | 'restrict';
REVOKE: 'REVOKE' | 'revoke';
RIGHT: 'RIGHT' | 'right';
ROLE: 'ROLE' | 'role';
ROLES: 'ROLES' | 'roles';
ROLLBACK: 'ROLLBACK' | 'rollback';
ROLLUP: 'ROLLUP' | 'rollup';
ROW: 'ROW' | 'row';
ROWS: 'ROWS' | 'rows';
RUNNING: 'RUNNING' | 'running';
SCHEMA: 'SCHEMA' | 'schema';
SCHEMAS: 'SCHEMAS' | 'schemas';
SECOND: 'SECOND' | 'second';
SECURITY: 'SECURITY' | 'security';
SEEK: 'SEEK' | 'seek';
SELECT: 'SELECT' | 'select';
SERIALIZABLE: 'SERIALIZABLE' | 'serializable';
SESSION: 'SESSION' | 'session';
SET: 'SET' | 'set';
SETS: 'SETS' | 'sets';
SHOW: 'SHOW' | 'show';
SOME: 'SOME' | 'some';
START: 'START' | 'start';
STATS: 'STATS' | 'stats';
SUBSET: 'SUBSET' | 'subset';
SUBSTRING: 'SUBSTRING' | 'substring';
SYSTEM: 'SYSTEM' | 'system';
TABLE: 'TABLE' | 'table';
TABLES: 'TABLES' | 'tables';
TABLESAMPLE: 'TABLESAMPLE' | 'tablesample';
TEXT: 'TEXT' | 'text';
THEN: 'THEN' | 'then';
TIES: 'TIES' | 'ties';
TIME: 'TIME' | 'time';
TIMESTAMP: 'TIMESTAMP' | 'timestamp';
TO: 'TO' | 'to';
TRANSACTION: 'TRANSACTION' | 'transaction';
TRUE: 'TRUE' | 'true';
TRUNCATE: 'TRUNCATE' | 'truncate';
TRY_CAST: 'TRY_CAST' | 'try_cast';
TYPE: 'TYPE' | 'type';
UESCAPE: 'UESCAPE' | 'uescape';
UNBOUNDED: 'UNBOUNDED' | 'unbounded';
UNCOMMITTED: 'UNCOMMITTED' | 'uncommitted';
UNION: 'UNION' | 'union';
UNMATCHED: 'UNMATCHED' | 'unmatched';
UNNEST: 'UNNEST' | 'unnest';
UPDATE: 'UPDATE' | 'update';
USE: 'USE' | 'use';
USER: 'USER' | 'user';
USING: 'USING' | 'using';
VALIDATE: 'VALIDATE' | 'validate';
VALUES: 'VALUES' | 'values';
VERBOSE: 'VERBOSE' | 'verbose';
VERSION: 'VERSION' | 'version';
VIEW: 'VIEW' | 'view';
WHEN: 'WHEN' | 'when';
WHERE: 'WHERE' | 'where';
WINDOW: 'WINDOW' | 'window';
WITH: 'WITH' | 'with';
WITHIN: 'WITHIN' | 'within';
WITHOUT: 'WITHOUT' | 'without';
WORK: 'WORK' | 'work';
WRITE: 'WRITE' | 'write';
YEAR: 'YEAR' | 'year';
ZONE: 'ZONE' | 'zone';

EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
QUESTION_MARK: '?';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    : 'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;

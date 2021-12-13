/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") {
        return true;
    }
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wang.yeting.sql.visitor;

import wang.yeting.sql.dialect.hive.ast.HiveInputOutputFormat;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateTableStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.SQLAlterResourceGroupStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.SQLCreateResourceGroupStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.SQLListResourceGroupStatement;

public interface SQLASTVisitor {

    default void endVisit(wang.yeting.sql.ast.expr.SQLAllColumnExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLBetweenExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCaseExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCaseExpr.Item x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCaseStatement x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCaseStatement.Item x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCharExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLInListExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLSmallIntExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLBigIntExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLTinyIntExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLExistsExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLNCharExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLNotExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLNullExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLNumberExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLRealExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSelectGroupByClause x) {
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSelectStatement x) {
    }

    default void postVisit(wang.yeting.sql.ast.SQLObject x) {
    }

    default void preVisit(wang.yeting.sql.ast.SQLObject x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLAllColumnExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBetweenExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCaseExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCaseExpr.Item x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCaseStatement x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCaseStatement.Item x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCastExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCharExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLExistsExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLInListExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLSmallIntExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBigIntExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLTinyIntExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLNCharExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLNotExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLNullExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLNumberExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLRealExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectGroupByClause x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCastExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLAggregateExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLAggregateExpr x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLVariantRefExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLVariantRefExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLQueryExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLQueryExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLUnaryExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLUnaryExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLHexExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLHexExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelect x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSelect select) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectQueryBlock x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSelectQueryBlock x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExprTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExprTableSource x) {
    }

    default boolean visit(wang.yeting.sql.ast.SQLOrderBy x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLOrderBy x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLZOrderBy x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLZOrderBy x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectOrderByItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSelectOrderByItem x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLColumnDefinition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLColumnDefinition x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLColumnDefinition.Identity x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLColumnDefinition.Identity x) {
    }

    default boolean visit(wang.yeting.sql.ast.SQLDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLDataType x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCharacterDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCharacterDataType x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDeleteStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDeleteStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCurrentOfCursorExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLCurrentOfCursorExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLInsertStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLInsertStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLInsertStatement.ValuesClause x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLInsertStatement.ValuesClause x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUpdateSetItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUpdateSetItem x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUpdateStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUpdateStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateViewStatement.Column x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateViewStatement.Column x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLNotNullConstraint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLNotNullConstraint x) {
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUnionQuery x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUnionQuery x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSetStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSetStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAssignItem x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAssignItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCallStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCallStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLJoinTableSource x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLJoinTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLJoinTableSource.UDJ x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLJoinTableSource.UDJ x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLSomeExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLSomeExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLAnyExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLAnyExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLAllExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLAllExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLInSubQueryExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLInSubQueryExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLListExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLListExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSubqueryTableSource x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSubqueryTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLTruncateStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLTruncateStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLDefaultExpr x) {
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLDefaultExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCommentStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCommentStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUseStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUseStatement x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddColumn x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddColumn x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDeleteByCondition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDeleteByCondition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableModifyClusteredBy x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableModifyClusteredBy x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropColumnItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropColumnItem x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropIndex x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropIndex x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableGroupStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableGroupStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterSystemSetConfigStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterSystemSetConfigStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterSystemGetConfigStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterSystemGetConfigStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropViewStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSavePointStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSavePointStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLRollbackStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLRollbackStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLReleaseSavePointStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLReleaseSavePointStatement x) {
    }

    default void endVisit(wang.yeting.sql.ast.SQLCommentHint x) {
    }

    default boolean visit(wang.yeting.sql.ast.SQLCommentHint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateDatabaseStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateDatabaseStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLOver x) {
    }

    default boolean visit(wang.yeting.sql.ast.SQLOver x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLKeep x) {
    }

    default boolean visit(wang.yeting.sql.ast.SQLKeep x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLColumnPrimaryKey x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLColumnPrimaryKey x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLColumnUniqueKey x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLColumnUniqueKey x) {
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLWithSubqueryClause x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLWithSubqueryClause x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAlterColumn x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAlterColumn x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCheck x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCheck x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDefault x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDefault x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropForeignKey x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropForeignKey x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropPrimaryKey x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropPrimaryKey x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDisableKeys x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDisableKeys x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableEnableKeys x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableEnableKeys x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDisableConstraint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDisableConstraint x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableEnableConstraint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableEnableConstraint x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLColumnCheck x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLColumnCheck x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExprHint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExprHint x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropConstraint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropConstraint x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUnique x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUnique x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPrimaryKeyImpl x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPrimaryKeyImpl x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRenameColumn x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRenameColumn x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLColumnReference x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLColumnReference x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLForeignKeyImpl x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLForeignKeyImpl x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropTriggerStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropTriggerStatement x) {

    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropUserStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropUserStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExplainStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExplainStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLGrantStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLGrantStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropDatabaseStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropDatabaseStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLIndexOptions x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLIndexOptions x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLIndexDefinition x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLIndexDefinition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddIndex x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddIndex x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAlterIndex x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAlterIndex x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddConstraint x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddConstraint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateTriggerStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateTriggerStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropFunctionStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropFunctionStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropTableSpaceStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropTableSpaceStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropProcedureStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropProcedureStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLBooleanExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBooleanExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUnionQueryTableSource x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUnionQueryTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLTimestampExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLTimestampExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLDateTimeExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLDateTimeExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLDoubleExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLDoubleExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLFloatExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLFloatExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLRevokeStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLRevokeStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLBinaryExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBinaryExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRename x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRename x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterViewRenameStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterViewRenameStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowTablesStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowTablesStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddExtPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddExtPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropExtPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropExtPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRenamePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRenamePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableSetComment x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSetComment x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableSetLifecycle x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPrivilegeItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPrivilegeItem x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSetLifecycle x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableEnableLifecycle x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSetLocation x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableSetLocation x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableEnableLifecycle x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTablePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTablePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTablePartitionSetProperties x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTablePartitionSetProperties x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDisableLifecycle x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDisableLifecycle x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableTouch x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableTouch x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLArrayExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLArrayExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLOpenStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLOpenStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLFetchStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLFetchStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCloseStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCloseStatement x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLGroupingSetExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLGroupingSetExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLIfStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLIfStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLIfStatement.ElseIf x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLIfStatement.ElseIf x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLIfStatement.Else x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLIfStatement.Else x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLLoopStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLLoopStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLParameter x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLParameter x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateProcedureStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateProcedureStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLBlockStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLBlockStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropKey x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropKey x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLDeclareItem x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLDeclareItem x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionValue x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionValue x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionByRange x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionByRange x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionByHash x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionByHash x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionByList x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionByList x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLSubPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLSubPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLSubPartitionByHash x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLSubPartitionByHash x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLSubPartitionByRange x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLSubPartitionByRange x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLSubPartitionByList x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLSubPartitionByList x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterDatabaseStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterDatabaseStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableConvertCharSet x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableConvertCharSet x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableReOrganizePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableReOrganizePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableCoalescePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableCoalescePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableTruncatePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableTruncatePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDiscardPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDiscardPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableImportPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableImportPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAnalyzePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAnalyzePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableCheckPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableCheckPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableOptimizePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableOptimizePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRebuildPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRebuildPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRepairPartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRepairPartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLSequenceExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLSequenceExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLMergeStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLMergeStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLMergeStatement.MergeUpdateClause x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLMergeStatement.MergeUpdateClause x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLMergeStatement.MergeInsertClause x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLMergeStatement.MergeInsertClause x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLErrorLoggingClause x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLErrorLoggingClause x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLNullConstraint x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLNullConstraint x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLDateExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLDateExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLLimit x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLLimit x) {
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLStartTransactionStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLStartTransactionStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDescribeStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDescribeStatement x) {
        return true;
    }

    /**
     * support procedure
     */
    default boolean visit(wang.yeting.sql.ast.statement.SQLWhileStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLWhileStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDeclareStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDeclareStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLReturnStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLReturnStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLArgument x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLArgument x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCommitStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCommitStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLFlashbackExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLFlashbackExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateMaterializedViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateMaterializedViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowCreateMaterializedViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowCreateMaterializedViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBinaryOpExprGroup x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLBinaryOpExprGroup x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLScriptCommitStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLScriptCommitStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLReplaceStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLReplaceStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateUserStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateUserStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterFunctionStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterFunctionStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTypeStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTypeStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLIntervalExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLIntervalExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLLateralViewTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLLateralViewTableSource x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowErrorsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowErrorsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowGrantsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowGrantsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowPackagesStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowPackagesStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowRecylebinStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowRecylebinStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterCharacter x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterCharacter x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExprStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExprStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterProcedureStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterProcedureStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropEventStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropEventStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropLogFileGroupStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropLogFileGroupStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropServerStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropServerStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropSynonymStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropSynonymStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLRecordDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLRecordDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropTypeStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropTypeStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExternalRecordFormat x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExternalRecordFormat x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLArrayDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLArrayDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLMapDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLMapDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLStructDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLStructDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLRowDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLRowDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLStructDataType.Field x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLStructDataType.Field x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropMaterializedViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropMaterializedViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowMaterializedViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowMaterializedViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLRefreshMaterializedViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLRefreshMaterializedViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterMaterializedViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterMaterializedViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableGroupStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateTableGroupStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropTableGroupStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropTableGroupStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSubpartitionAvailablePartitionNum x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableSubpartitionAvailablePartitionNum x) {

    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowDatabasesStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowDatabasesStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowTableGroupsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowTableGroupsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowColumnsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowColumnsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowCreateTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowCreateTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowProcessListStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowProcessListStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableSetOption x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSetOption x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowCreateViewStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowCreateViewStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowViewsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowViewsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRenameIndex x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRenameIndex x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterSequenceStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterSequenceStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableExchangePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableExchangePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateRoleStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateRoleStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropRoleStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropRoleStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableReplaceColumn x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableReplaceColumn x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLMatchAgainstExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLMatchAgainstExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLTimeExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLTimeExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropCatalogStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropCatalogStatement x) {

    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowPartitionsStmt x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowPartitionsStmt x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLValuesExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLValuesExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLContainsExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLContainsExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDumpStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDumpStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLValuesTableSource x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLValuesTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLExtractExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLExtractExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLWindow x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLWindow x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLJSONExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLJSONExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLDecimalExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLDecimalExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLAnnIndex x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLAnnIndex x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLUnionDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLUnionDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableRecoverPartitions x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRecoverPartitions x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterIndexStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterIndexStatement x) {
        return true;
    }


    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterIndexStatement.Rebuild x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterIndexStatement.Rebuild x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowIndexesStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowIndexesStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAnalyzeTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAnalyzeTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExportTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExportTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLImportTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLImportTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLTableSampling x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLTableSampling x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLSizeExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLSizeExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableArchivePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableArchivePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableUnarchivePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableUnarchivePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateOutlineStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCreateOutlineStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropOutlineStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropOutlineStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterOutlineStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterOutlineStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowOutlinesStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowOutlinesStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPurgeTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPurgeTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPurgeTemporaryOutputStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPurgeTemporaryOutputStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPurgeLogsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPurgeLogsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPurgeRecyclebinStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPurgeRecyclebinStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowStatisticStmt x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowStatisticStmt x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowStatisticListStmt x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowStatisticListStmt x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddSupplemental x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddSupplemental x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowCatalogsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowCatalogsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowFunctionsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowFunctionsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowSessionStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowSessionStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLDbLinkExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.expr.SQLDbLinkExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLCurrentTimeExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLCurrentTimeExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLCurrentUserExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLCurrentUserExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowQueryTaskStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowQueryTaskStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLAdhocTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLAdhocTableSource x) {

    }

    default boolean visit(HiveCreateTableStatement x) {
        return true;
    }

    default void endVisit(HiveCreateTableStatement x) {

    }

    default boolean visit(HiveInputOutputFormat x) {
        return true;
    }

    default void endVisit(HiveInputOutputFormat x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExplainAnalyzeStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExplainAnalyzeStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPartitionRef x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPartitionRef x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLPartitionRef.Item x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLPartitionRef.Item x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLWhoamiStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLWhoamiStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropResourceStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropResourceStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLForStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLForStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUnnestTableSource x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLUnnestTableSource x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCopyFromStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCopyFromStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowUsersStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowUsersStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSubmitJobStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSubmitJobStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLTableLike x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLTableLike x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSyncMetaStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLSyncMetaStatement x) {

    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLValuesQuery x) {
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLValuesQuery x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLDataTypeRefExpr x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLDataTypeRefExpr x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLArchiveTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLArchiveTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLBackupStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLBackupStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLRestoreStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLRestoreStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLBuildTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLBuildTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCancelJobStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCancelJobStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLExportDatabaseStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExportDatabaseStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLImportDatabaseStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLImportDatabaseStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLRenameUserStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLRenameUserStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionByValue x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionByValue x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTablePartitionCount x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTablePartitionCount x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableBlockSize x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableBlockSize x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableCompression x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableCompression x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTablePartitionLifecycle x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTablePartitionLifecycle x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableSubpartitionLifecycle x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSubpartitionLifecycle x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropSubpartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropSubpartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableDropClusteringKey x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropClusteringKey x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddClusteringKey x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddClusteringKey x) {
        return true;
    }

    default void endVisit(MySqlKillStatement x) {

    }

    default boolean visit(MySqlKillStatement x) {
        return true;
    }


    default boolean visit(SQLCreateResourceGroupStatement x) {
        return true;
    }

    default void endVisit(SQLCreateResourceGroupStatement x) {

    }

    default boolean visit(SQLAlterResourceGroupStatement x) {
        return true;
    }

    default void endVisit(SQLAlterResourceGroupStatement x) {

    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLDropResourceGroupStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDropResourceGroupStatement x) {
        return true;
    }

    default void endVisit(SQLListResourceGroupStatement x) {

    }

    default boolean visit(SQLListResourceGroupStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableMergePartition x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableMergePartition x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionSpec x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionSpec x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLPartitionSpec.Item x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLPartitionSpec.Item x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableChangeOwner x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableChangeOwner x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.SQLTableDataType x) {

    }

    default boolean visit(wang.yeting.sql.ast.SQLTableDataType x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLCloneTableStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCloneTableStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowHistoryStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowHistoryStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowRoleStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowRoleStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowRolesStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowRolesStatement x) {
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowVariantsStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowVariantsStatement x) {

    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLShowACLStatement x) {
        return true;
    }

    default void endVisit(wang.yeting.sql.ast.statement.SQLShowACLStatement x) {

    }

}

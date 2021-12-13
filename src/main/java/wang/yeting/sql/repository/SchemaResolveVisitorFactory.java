/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package wang.yeting.sql.repository;

import wang.yeting.sql.SQLUtils;
import wang.yeting.sql.dialect.db2.ast.DB2Object;
import wang.yeting.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import wang.yeting.sql.dialect.db2.visitor.DB2ASTVisitorAdapter;
import wang.yeting.sql.dialect.hive.ast.HiveInsert;
import wang.yeting.sql.dialect.hive.ast.HiveInsertStatement;
import wang.yeting.sql.dialect.hive.ast.HiveMultiInsertStatement;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateTableStatement;
import wang.yeting.sql.dialect.hive.visitor.HiveASTVisitorAdapter;
import wang.yeting.sql.dialect.mysql.ast.MysqlForeignKey;
import wang.yeting.sql.dialect.mysql.ast.clause.MySqlCursorDeclareStatement;
import wang.yeting.sql.dialect.mysql.ast.clause.MySqlDeclareStatement;
import wang.yeting.sql.dialect.mysql.ast.clause.MySqlRepeatStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.*;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import wang.yeting.sql.dialect.odps.ast.OdpsCreateTableStatement;
import wang.yeting.sql.dialect.odps.ast.OdpsSelectQueryBlock;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitorAdapter;
import wang.yeting.sql.dialect.oracle.ast.stmt.*;
import wang.yeting.sql.dialect.oracle.visitor.OracleASTVisitorAdapter;
import wang.yeting.sql.dialect.postgresql.ast.stmt.*;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitorAdapter;
import wang.yeting.sql.dialect.sqlserver.ast.SQLServerSelectQueryBlock;
import wang.yeting.sql.dialect.sqlserver.ast.stmt.SQLServerInsertStatement;
import wang.yeting.sql.dialect.sqlserver.ast.stmt.SQLServerUpdateStatement;
import wang.yeting.sql.dialect.sqlserver.visitor.SQLServerASTVisitorAdapter;
import wang.yeting.sql.util.FnvHash;
import wang.yeting.sql.util.PGUtils;
import wang.yeting.sql.visitor.SQLASTVisitorAdapter;

import java.util.ArrayList;
import java.util.List;

class SchemaResolveVisitorFactory {
    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTableSource();
        ctx.setTableSource(table);

        table.accept(visitor);

        List<wang.yeting.sql.ast.statement.SQLTableElement> elements = x.getTableElementList();
        for (int i = 0; i < elements.size(); i++) {
            wang.yeting.sql.ast.statement.SQLTableElement e = elements.get(i);
            if (e instanceof wang.yeting.sql.ast.statement.SQLColumnDefinition) {
                wang.yeting.sql.ast.statement.SQLColumnDefinition columnn = (wang.yeting.sql.ast.statement.SQLColumnDefinition) e;
                wang.yeting.sql.ast.SQLName columnnName = columnn.getName();
                if (columnnName instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) columnnName;
                    identifierExpr.setResolvedTableSource(table);
                    identifierExpr.setResolvedColumn(columnn);
                }
            } else if (e instanceof wang.yeting.sql.ast.statement.SQLUniqueConstraint) {
                List<wang.yeting.sql.ast.statement.SQLSelectOrderByItem> columns = ((wang.yeting.sql.ast.statement.SQLUniqueConstraint) e).getColumns();
                for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem orderByItem : columns) {
                    wang.yeting.sql.ast.SQLExpr orderByItemExpr = orderByItem.getExpr();
                    if (orderByItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                        wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) orderByItemExpr;
                        identifierExpr.setResolvedTableSource(table);

                        wang.yeting.sql.ast.statement.SQLColumnDefinition column = x.findColumn(identifierExpr.nameHashCode64());
                        if (column != null) {
                            identifierExpr.setResolvedColumn(column);
                        }
                    }
                }
            } else {
                e.accept(visitor);
            }
        }

        wang.yeting.sql.ast.statement.SQLSelect select = x.getSelect();
        if (select != null) {
            visitor.visit(select);
        }

        SchemaRepository repository = visitor.getRepository();
        if (repository != null) {
            repository.acceptCreateTable(x);
        }

        visitor.popContext();

        wang.yeting.sql.ast.statement.SQLExprTableSource like = x.getLike();
        if (like != null) {
            like.accept(visitor);
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLUpdateStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = x.getWith();
        if (with != null) {
            with.accept(visitor);
        }

        wang.yeting.sql.ast.statement.SQLTableSource table = x.getTableSource();
        wang.yeting.sql.ast.statement.SQLTableSource from = x.getFrom();

        ctx.setTableSource(table);
        ctx.setFrom(from);

        table.accept(visitor);
        if (from != null) {
            from.accept(visitor);
        }

        List<wang.yeting.sql.ast.statement.SQLUpdateSetItem> items = x.getItems();
        for (wang.yeting.sql.ast.statement.SQLUpdateSetItem item : items) {
            wang.yeting.sql.ast.SQLExpr column = item.getColumn();
            if (column instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) column;
                identifierExpr.setResolvedTableSource(table);
                visitor.visit(identifierExpr);
            } else if (column instanceof wang.yeting.sql.ast.expr.SQLListExpr) {
                wang.yeting.sql.ast.expr.SQLListExpr columnGroup = (wang.yeting.sql.ast.expr.SQLListExpr) column;
                for (wang.yeting.sql.ast.SQLExpr columnGroupItem : columnGroup.getItems()) {
                    if (columnGroupItem instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                        wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) columnGroupItem;
                        identifierExpr.setResolvedTableSource(table);
                        visitor.visit(identifierExpr);
                    } else {
                        columnGroupItem.accept(visitor);
                    }
                }
            } else {
                column.accept(visitor);
            }
            wang.yeting.sql.ast.SQLExpr value = item.getValue();
            if (value != null) {
                value.accept(visitor);
            }
        }

        wang.yeting.sql.ast.SQLExpr where = x.getWhere();
        if (where != null) {
            where.accept(visitor);
        }

        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            orderBy.accept(visitor);
        }

        for (wang.yeting.sql.ast.SQLExpr sqlExpr : x.getReturning()) {
            sqlExpr.accept(visitor);
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLDeleteStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = x.getWith();
        if (with != null) {
            visitor.visit(with);
        }

        wang.yeting.sql.ast.statement.SQLTableSource table = x.getTableSource();
        wang.yeting.sql.ast.statement.SQLTableSource from = x.getFrom();

        if (from == null) {
            from = x.getUsing();
        }

        if (table == null && from != null) {
            table = from;
            from = null;
        }

        if (from != null) {
            ctx.setFrom(from);
            from.accept(visitor);
        }

        if (table != null) {
            if (from != null && table instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                wang.yeting.sql.ast.SQLExpr tableExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) table).getExpr();
                if (tableExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr
                        && ((wang.yeting.sql.ast.expr.SQLPropertyExpr) tableExpr).getName().equals("*")) {
                    String alias = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) tableExpr).getOwnernName();
                    wang.yeting.sql.ast.statement.SQLTableSource refTableSource = from.findTableSource(alias);
                    if (refTableSource != null) {
                        ((wang.yeting.sql.ast.expr.SQLPropertyExpr) tableExpr).setResolvedTableSource(refTableSource);
                    }
                }
            }
            table.accept(visitor);
            ctx.setTableSource(table);
        }

        wang.yeting.sql.ast.SQLExpr where = x.getWhere();
        if (where != null) {
            where.accept(visitor);
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLInsertStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = x.getWith();
        if (with != null) {
            visitor.visit(with);
        }

        wang.yeting.sql.ast.statement.SQLTableSource table = x.getTableSource();

        ctx.setTableSource(table);

        if (table != null) {
            table.accept(visitor);
        }

        for (wang.yeting.sql.ast.SQLExpr column : x.getColumns()) {
            column.accept(visitor);
        }

        if (x instanceof HiveInsertStatement) {
            for (wang.yeting.sql.ast.statement.SQLAssignItem item : ((HiveInsertStatement) x).getPartitions()) {
                item.accept(visitor);
            }
        }

        for (wang.yeting.sql.ast.statement.SQLInsertStatement.ValuesClause valuesClause : x.getValuesList()) {
            valuesClause.accept(visitor);
        }

        wang.yeting.sql.ast.statement.SQLSelect query = x.getQuery();
        if (query != null) {
            visitor.visit(query);
        }

        visitor.popContext();
    }

    static void resolveIdent(SchemaResolveVisitor visitor, wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        SchemaResolveVisitor.Context ctx = visitor.getContext();
        if (ctx == null) {
            return;
        }

        String ident = x.getName();
        long hash = x.nameHashCode64();
        wang.yeting.sql.ast.statement.SQLTableSource tableSource = null;

        if ((hash == FnvHash.Constants.LEVEL || hash == FnvHash.Constants.CONNECT_BY_ISCYCLE)
                && ctx.object instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) ctx.object;
            if (queryBlock.getStartWith() != null
                    || queryBlock.getConnectBy() != null) {
                return;
            }
        }

        wang.yeting.sql.ast.statement.SQLTableSource ctxTable = ctx.getTableSource();

        if (ctxTable instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
            wang.yeting.sql.ast.statement.SQLJoinTableSource join = (wang.yeting.sql.ast.statement.SQLJoinTableSource) ctxTable;
            tableSource = join.findTableSourceWithColumn(hash, ident, visitor.getOptions());
            if (tableSource == null) {
                final wang.yeting.sql.ast.statement.SQLTableSource left = join.getLeft(), right = join.getRight();

                if (left instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                        && right instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    wang.yeting.sql.ast.statement.SQLSelect leftSelect = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) left).getSelect();
                    if (leftSelect.getQuery() instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                        boolean hasAllColumn = ((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) leftSelect.getQuery()).selectItemHasAllColumn();
                        if (!hasAllColumn) {
                            tableSource = right;
                        }
                    }
                } else if (right instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                        && left instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    wang.yeting.sql.ast.statement.SQLSelect rightSelect = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) right).getSelect();
                    if (rightSelect.getQuery() instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                        boolean hasAllColumn = ((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) rightSelect.getQuery()).selectItemHasAllColumn();
                        if (!hasAllColumn) {
                            tableSource = left;
                        }
                    }
                } else if (left instanceof wang.yeting.sql.ast.statement.SQLExprTableSource && right instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    wang.yeting.sql.ast.statement.SQLExprTableSource leftExprTableSource = (wang.yeting.sql.ast.statement.SQLExprTableSource) left;
                    wang.yeting.sql.ast.statement.SQLExprTableSource rightExprTableSource = (wang.yeting.sql.ast.statement.SQLExprTableSource) right;

                    if (leftExprTableSource.getSchemaObject() != null
                            && rightExprTableSource.getSchemaObject() == null) {
                        tableSource = rightExprTableSource;

                    } else if (rightExprTableSource.getSchemaObject() != null
                            && leftExprTableSource.getSchemaObject() == null) {
                        tableSource = leftExprTableSource;
                    }
                }
            }
        } else if (ctxTable instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
            tableSource = ctxTable.findTableSourceWithColumn(hash, ident, visitor.getOptions());
        } else if (ctxTable instanceof wang.yeting.sql.ast.statement.SQLLateralViewTableSource) {
            tableSource = ctxTable.findTableSourceWithColumn(hash, ident, visitor.getOptions());

            if (tableSource == null) {
                tableSource = ((wang.yeting.sql.ast.statement.SQLLateralViewTableSource) ctxTable).getTableSource();
            }
        } else {
            for (SchemaResolveVisitor.Context parentCtx = ctx;
                 parentCtx != null;
                 parentCtx = parentCtx.parent) {
                wang.yeting.sql.ast.SQLDeclareItem declareItem = parentCtx.findDeclare(hash);
                if (declareItem != null) {
                    x.setResolvedDeclareItem(declareItem);
                    return;
                }

                if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLBlockStatement) {
                    wang.yeting.sql.ast.statement.SQLBlockStatement block = (wang.yeting.sql.ast.statement.SQLBlockStatement) parentCtx.object;
                    wang.yeting.sql.ast.SQLParameter parameter = block.findParameter(hash);
                    if (parameter != null) {
                        x.setResolvedParameter(parameter);
                        return;
                    }
                } else if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLCreateProcedureStatement) {
                    wang.yeting.sql.ast.statement.SQLCreateProcedureStatement createProc = (wang.yeting.sql.ast.statement.SQLCreateProcedureStatement) parentCtx.object;
                    wang.yeting.sql.ast.SQLParameter parameter = createProc.findParameter(hash);
                    if (parameter != null) {
                        x.setResolvedParameter(parameter);
                        return;
                    }
                }
            }

            tableSource = ctxTable;
            if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                SchemaObject table = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getSchemaObject();
                if (table != null) {
                    if (table.findColumn(hash) == null) {
                        wang.yeting.sql.ast.statement.SQLCreateTableStatement createStmt = null;
                        {
                            wang.yeting.sql.ast.SQLStatement smt = table.getStatement();
                            if (smt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                                createStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) smt;
                            }
                        }

                        if (createStmt != null && createStmt.getTableElementList().size() > 0) {
                            tableSource = null; // maybe parent
                        }
                    }
                }
            }
        }

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();

            if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr func = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if (func.methodNameHashCode64() == FnvHash.Constants.ANN) {
                    expr = func.getArguments().get(0);
                }
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr identExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
                long identHash = identExpr.nameHashCode64();

                tableSource = unwrapAlias(ctx, tableSource, identHash);
            }
        }

        if (tableSource != null) {
            x.setResolvedTableSource(tableSource);

            wang.yeting.sql.ast.statement.SQLColumnDefinition column = tableSource.findColumn(hash);
            if (column != null) {
                x.setResolvedColumn(column);
            }

            if (ctxTable instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
                String alias = tableSource.computeAlias();
                if (alias == null || tableSource instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) {
                    return;
                }

                if (visitor.isEnabled(SchemaResolveVisitor.Option.ResolveIdentifierAlias)) {
                    wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = new wang.yeting.sql.ast.expr.SQLPropertyExpr(new wang.yeting.sql.ast.expr.SQLIdentifierExpr(alias), ident, hash);
                    propertyExpr.setResolvedColumn(x.getResolvedColumn());
                    propertyExpr.setResolvedTableSource(x.getResolvedTableSource());
                    SQLUtils.replaceInParent(x, propertyExpr);
                }
            }
        }

        if (x.getResolvedColumn() == null
                && x.getResolvedTableSource() == null) {
            for (SchemaResolveVisitor.Context parentCtx = ctx;
                 parentCtx != null;
                 parentCtx = parentCtx.parent) {
                wang.yeting.sql.ast.SQLDeclareItem declareItem = parentCtx.findDeclare(hash);
                if (declareItem != null) {
                    x.setResolvedDeclareItem(declareItem);
                    return;
                }

                if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLBlockStatement) {
                    wang.yeting.sql.ast.statement.SQLBlockStatement block = (wang.yeting.sql.ast.statement.SQLBlockStatement) parentCtx.object;
                    wang.yeting.sql.ast.SQLParameter parameter = block.findParameter(hash);
                    if (parameter != null) {
                        x.setResolvedParameter(parameter);
                        return;
                    }
                } else if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLCreateProcedureStatement) {
                    wang.yeting.sql.ast.statement.SQLCreateProcedureStatement createProc = (wang.yeting.sql.ast.statement.SQLCreateProcedureStatement) parentCtx.object;
                    wang.yeting.sql.ast.SQLParameter parameter = createProc.findParameter(hash);
                    if (parameter != null) {
                        x.setResolvedParameter(parameter);
                        return;
                    }
                }
            }
        }

        if (x.getResolvedColumnObject() == null && ctx.object instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) ctx.object;
            boolean having = false;
            for (wang.yeting.sql.ast.SQLObject current = x, parent = x.getParent(); parent != null; current = parent, parent = parent.getParent()) {
                if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectGroupByClause && parent.getParent() == queryBlock) {
                    wang.yeting.sql.ast.statement.SQLSelectGroupByClause groupBy = (wang.yeting.sql.ast.statement.SQLSelectGroupByClause) parent;
                    if (current == groupBy.getHaving()) {
                        having = true;
                    }
                    break;
                }
            }
            if (having) {
                wang.yeting.sql.ast.statement.SQLSelectItem selectItem = queryBlock.findSelectItem(x.hashCode64());
                if (selectItem != null) {
                    x.setResolvedColumn(selectItem);
                }
            }
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
        SchemaResolveVisitor.Context ctx = visitor.getContext();
        if (ctx == null) {
            return;
        }

        long owner_hash = 0;
        {
            wang.yeting.sql.ast.SQLExpr ownerObj = x.getOwner();
            if (ownerObj instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr owner = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) ownerObj;
                owner_hash = owner.nameHashCode64();
            } else if (ownerObj instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                owner_hash = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) ownerObj).hashCode64();
            }
        }

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = null;
        wang.yeting.sql.ast.statement.SQLTableSource ctxTable = ctx.getTableSource();

        if (ctxTable != null) {
            tableSource = ctxTable.findTableSource(owner_hash);
        }

        if (tableSource == null) {
            wang.yeting.sql.ast.statement.SQLTableSource ctxFrom = ctx.getFrom();
            if (ctxFrom != null) {
                tableSource = ctxFrom.findTableSource(owner_hash);
            }
        }

        if (tableSource == null) {
            for (SchemaResolveVisitor.Context parentCtx = ctx;
                 parentCtx != null;
                 parentCtx = parentCtx.parent) {

                wang.yeting.sql.ast.statement.SQLTableSource parentCtxTable = parentCtx.getTableSource();

                if (parentCtxTable != null) {
                    tableSource = parentCtxTable.findTableSource(owner_hash);
                    if (tableSource == null) {
                        wang.yeting.sql.ast.statement.SQLTableSource ctxFrom = parentCtx.getFrom();
                        if (ctxFrom != null) {
                            tableSource = ctxFrom.findTableSource(owner_hash);
                        }
                    }

                    if (tableSource != null) {
                        break;
                    }
                } else {
                    if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLBlockStatement) {
                        wang.yeting.sql.ast.statement.SQLBlockStatement block = (wang.yeting.sql.ast.statement.SQLBlockStatement) parentCtx.object;
                        wang.yeting.sql.ast.SQLParameter parameter = block.findParameter(owner_hash);
                        if (parameter != null) {
                            x.setResolvedOwnerObject(parameter);
                            return;
                        }
                    } else if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLMergeStatement) {
                        wang.yeting.sql.ast.statement.SQLMergeStatement mergeStatement = (wang.yeting.sql.ast.statement.SQLMergeStatement) parentCtx.object;
                        wang.yeting.sql.ast.statement.SQLTableSource into = mergeStatement.getInto();
                        if (into instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                                && into.aliasHashCode64() == owner_hash) {
                            x.setResolvedOwnerObject(into);
                        }
                    }

                    wang.yeting.sql.ast.SQLDeclareItem declareItem = parentCtx.findDeclare(owner_hash);
                    if (declareItem != null) {
                        wang.yeting.sql.ast.SQLObject resolvedObject = declareItem.getResolvedObject();
                        if (resolvedObject instanceof wang.yeting.sql.ast.statement.SQLCreateProcedureStatement
                                || resolvedObject instanceof wang.yeting.sql.ast.statement.SQLCreateFunctionStatement
                                || resolvedObject instanceof wang.yeting.sql.ast.statement.SQLTableSource) {
                            x.setResolvedOwnerObject(resolvedObject);
                        }
                        break;
                    }
                }
            }
        }

        if (tableSource != null) {
            x.setResolvedTableSource(tableSource);
            wang.yeting.sql.ast.SQLObject column = tableSource.resolveColum(
                    x.nameHashCode64());
            if (column instanceof wang.yeting.sql.ast.statement.SQLColumnDefinition) {
                x.setResolvedColumn((wang.yeting.sql.ast.statement.SQLColumnDefinition) column);
            } else if (column instanceof wang.yeting.sql.ast.statement.SQLSelectItem) {
                x.setResolvedColumn((wang.yeting.sql.ast.statement.SQLSelectItem) column);
            }
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
        final wang.yeting.sql.ast.expr.SQLBinaryOperator op = x.getOperator();
        final wang.yeting.sql.ast.SQLExpr left = x.getLeft();

        if ((op == wang.yeting.sql.ast.expr.SQLBinaryOperator.BooleanAnd || op == wang.yeting.sql.ast.expr.SQLBinaryOperator.BooleanOr)
                && left instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr
                && ((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) left).getOperator() == op) {
            List<wang.yeting.sql.ast.SQLExpr> groupList = wang.yeting.sql.ast.expr.SQLBinaryOpExpr.split(x, op);
            for (int i = 0; i < groupList.size(); i++) {
                wang.yeting.sql.ast.SQLExpr item = groupList.get(i);
                item.accept(visitor);
            }
            return;
        }

        if (left != null) {
            if (left instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                resolve(visitor, (wang.yeting.sql.ast.expr.SQLBinaryOpExpr) left);
            } else {
                left.accept(visitor);
            }
        }

        wang.yeting.sql.ast.SQLExpr right = x.getRight();
        if (right != null) {
            right.accept(visitor);
        }
    }

    static wang.yeting.sql.ast.statement.SQLTableSource unwrapAlias(SchemaResolveVisitor.Context ctx, wang.yeting.sql.ast.statement.SQLTableSource tableSource, long identHash) {
        if (ctx == null) {
            return tableSource;
        }

        if (ctx.object instanceof wang.yeting.sql.ast.statement.SQLDeleteStatement
                && (ctx.getTableSource() == null || tableSource == ctx.getTableSource())
                && ctx.getFrom() != null) {
            wang.yeting.sql.ast.statement.SQLTableSource found = ctx.getFrom().findTableSource(identHash);
            if (found != null) {
                return found;
            }
        }

        for (SchemaResolveVisitor.Context parentCtx = ctx;
             parentCtx != null;
             parentCtx = parentCtx.parent) {

            wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = null;
            if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLSelect) {
                wang.yeting.sql.ast.statement.SQLSelect select = (wang.yeting.sql.ast.statement.SQLSelect) parentCtx.object;
                with = select.getWithSubQuery();
            } else if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLDeleteStatement) {
                wang.yeting.sql.ast.statement.SQLDeleteStatement delete = (wang.yeting.sql.ast.statement.SQLDeleteStatement) parentCtx.object;
                with = delete.getWith();
            } else if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLInsertStatement) {
                wang.yeting.sql.ast.statement.SQLInsertStatement insertStmt = (wang.yeting.sql.ast.statement.SQLInsertStatement) parentCtx.object;
                with = insertStmt.getWith();
            } else if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLUpdateStatement) {
                wang.yeting.sql.ast.statement.SQLUpdateStatement updateStmt = (wang.yeting.sql.ast.statement.SQLUpdateStatement) parentCtx.object;
                with = updateStmt.getWith();
            }

            if (with != null) {
                wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry = with.findEntry(identHash);
                if (entry != null) {
                    return entry;
                }
            }
        }
        return tableSource;
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLSelectQueryBlock x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);
        if (ctx != null && ctx.level >= 32) {
            return;
        }

        wang.yeting.sql.ast.statement.SQLTableSource from = x.getFrom();

        if (from != null) {
            ctx.setTableSource(from);

            Class fromClass = from.getClass();
            if (fromClass == wang.yeting.sql.ast.statement.SQLExprTableSource.class) {
                visitor.visit((wang.yeting.sql.ast.statement.SQLExprTableSource) from);
            } else {
                from.accept(visitor);
            }
        } else if (x.getParent() != null && x.getParent().getParent() instanceof HiveInsert
                && x.getParent().getParent().getParent() instanceof HiveMultiInsertStatement) {
            HiveMultiInsertStatement insert = (HiveMultiInsertStatement) x.getParent().getParent().getParent();
            if (insert.getFrom() instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                from = insert.getFrom();
                ctx.setTableSource(from);
            }
        }

        List<wang.yeting.sql.ast.statement.SQLSelectItem> selectList = x.getSelectList();

        List<wang.yeting.sql.ast.statement.SQLSelectItem> columns = new ArrayList<wang.yeting.sql.ast.statement.SQLSelectItem>();
        for (int i = selectList.size() - 1; i >= 0; i--) {
            wang.yeting.sql.ast.statement.SQLSelectItem selectItem = selectList.get(i);
            wang.yeting.sql.ast.SQLExpr expr = selectItem.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLAllColumnExpr) {
                wang.yeting.sql.ast.expr.SQLAllColumnExpr allColumnExpr = (wang.yeting.sql.ast.expr.SQLAllColumnExpr) expr;
                allColumnExpr.setResolvedTableSource(from);

                visitor.visit(allColumnExpr);

                if (visitor.isEnabled(SchemaResolveVisitor.Option.ResolveAllColumn)) {
                    extractColumns(visitor, from, null, columns);
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr;
                visitor.visit(propertyExpr);

                String ownerName = propertyExpr.getOwnernName();
                if (propertyExpr.getName().equals("*")) {
                    if (visitor.isEnabled(SchemaResolveVisitor.Option.ResolveAllColumn)) {
                        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.findTableSource(ownerName);
                        extractColumns(visitor, tableSource, ownerName, columns);
                    }
                }

                wang.yeting.sql.ast.statement.SQLColumnDefinition column = propertyExpr.getResolvedColumn();
                if (column != null) {
                    continue;
                }
                wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.findTableSource(propertyExpr.getOwnernName());
                if (tableSource != null) {
                    column = tableSource.findColumn(propertyExpr.nameHashCode64());
                    if (column != null) {
                        propertyExpr.setResolvedColumn(column);
                    }
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr identExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
                visitor.visit(identExpr);

                long name_hash = identExpr.nameHashCode64();

                wang.yeting.sql.ast.statement.SQLColumnDefinition column = identExpr.getResolvedColumn();
                if (column != null) {
                    continue;
                }
                if (from == null) {
                    continue;
                }
                column = from.findColumn(name_hash);
                if (column != null) {
                    identExpr.setResolvedColumn(column);
                }
            } else {
                expr.accept(visitor);
            }

            if (columns.size() > 0) {
                for (wang.yeting.sql.ast.statement.SQLSelectItem column : columns) {
                    column.setParent(x);
                    column.getExpr().accept(visitor);
                }

                selectList.remove(i);
                selectList.addAll(i, columns);
                columns.clear();
            }
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource into = x.getInto();
        if (into != null) {
            visitor.visit(into);
        }

        wang.yeting.sql.ast.SQLExpr where = x.getWhere();
        if (where != null) {
            if (where instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                wang.yeting.sql.ast.expr.SQLBinaryOpExpr binaryOpExpr = (wang.yeting.sql.ast.expr.SQLBinaryOpExpr) where;
                resolveExpr(visitor, binaryOpExpr.getLeft());
                resolveExpr(visitor, binaryOpExpr.getRight());
            } else if (where instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExprGroup) {
                wang.yeting.sql.ast.expr.SQLBinaryOpExprGroup binaryOpExprGroup = (wang.yeting.sql.ast.expr.SQLBinaryOpExprGroup) where;
                for (wang.yeting.sql.ast.SQLExpr item : binaryOpExprGroup.getItems()) {
                    if (item instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                        wang.yeting.sql.ast.expr.SQLBinaryOpExpr binaryOpExpr = (wang.yeting.sql.ast.expr.SQLBinaryOpExpr) item;
                        resolveExpr(visitor, binaryOpExpr.getLeft());
                        resolveExpr(visitor, binaryOpExpr.getRight());
                    } else {
                        item.accept(visitor);
                    }
                }
            } else {
                where.accept(visitor);
            }
        }

        wang.yeting.sql.ast.SQLExpr startWith = x.getStartWith();
        if (startWith != null) {
            startWith.accept(visitor);
        }

        wang.yeting.sql.ast.SQLExpr connectBy = x.getConnectBy();
        if (connectBy != null) {
            connectBy.accept(visitor);
        }

        wang.yeting.sql.ast.statement.SQLSelectGroupByClause groupBy = x.getGroupBy();
        if (groupBy != null) {
            groupBy.accept(visitor);
        }

        List<wang.yeting.sql.ast.SQLWindow> windows = x.getWindows();
        if (windows != null) {
            for (wang.yeting.sql.ast.SQLWindow window : windows) {
                window.accept(visitor);
            }
        }

        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem orderByItem : orderBy.getItems()) {
                wang.yeting.sql.ast.SQLExpr orderByItemExpr = orderByItem.getExpr();

                if (orderByItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    wang.yeting.sql.ast.expr.SQLIdentifierExpr orderByItemIdentExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) orderByItemExpr;
                    long hash = orderByItemIdentExpr.nameHashCode64();
                    wang.yeting.sql.ast.statement.SQLSelectItem selectItem = x.findSelectItem(hash);

                    if (selectItem != null) {
                        orderByItem.setResolvedSelectItem(selectItem);

                        wang.yeting.sql.ast.SQLExpr selectItemExpr = selectItem.getExpr();
                        if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                            orderByItemIdentExpr.setResolvedTableSource(((wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr).getResolvedTableSource());
                            orderByItemIdentExpr.setResolvedColumn(((wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr).getResolvedColumn());
                        } else if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                            orderByItemIdentExpr.setResolvedTableSource(((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr).getResolvedTableSource());
                            orderByItemIdentExpr.setResolvedColumn(((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr).getResolvedColumn());
                        }
                        continue;
                    }
                }

                orderByItemExpr.accept(visitor);
            }
        }

        int forUpdateOfSize = x.getForUpdateOfSize();
        if (forUpdateOfSize > 0) {
            for (wang.yeting.sql.ast.SQLExpr sqlExpr : x.getForUpdateOf()) {
                sqlExpr.accept(visitor);
            }
        }

        List<wang.yeting.sql.ast.statement.SQLSelectOrderByItem> distributeBy = x.getDistributeBy();
        if (distributeBy != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : distributeBy) {
                item.accept(visitor);
            }
        }

        List<wang.yeting.sql.ast.statement.SQLSelectOrderByItem> sortBy = x.getSortBy();
        if (sortBy != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : sortBy) {
                item.accept(visitor);
            }
        }

        visitor.popContext();
    }

    static void extractColumns(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLTableSource from, String ownerName, List<wang.yeting.sql.ast.statement.SQLSelectItem> columns) {
        if (from instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) from).getExpr();

            SchemaRepository repository = visitor.getRepository();
            if (repository == null) {
                return;
            }

            String alias = from.getAlias();

            SchemaObject table = repository.findTable((wang.yeting.sql.ast.statement.SQLExprTableSource) from);
            if (table != null) {
                wang.yeting.sql.ast.statement.SQLCreateTableStatement createTableStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) table.getStatement();
                for (wang.yeting.sql.ast.statement.SQLTableElement e : createTableStmt.getTableElementList()) {
                    if (e instanceof wang.yeting.sql.ast.statement.SQLColumnDefinition) {
                        wang.yeting.sql.ast.statement.SQLColumnDefinition column = (wang.yeting.sql.ast.statement.SQLColumnDefinition) e;

                        if (alias != null) {
                            wang.yeting.sql.ast.expr.SQLPropertyExpr name = new wang.yeting.sql.ast.expr.SQLPropertyExpr(alias, column.getName().getSimpleName());
                            name.setResolvedColumn(column);
                            columns.add(new wang.yeting.sql.ast.statement.SQLSelectItem(name));
                        } else if (ownerName != null) {
                            wang.yeting.sql.ast.expr.SQLPropertyExpr name = new wang.yeting.sql.ast.expr.SQLPropertyExpr(ownerName, column.getName().getSimpleName());
                            name.setResolvedColumn(column);
                            columns.add(new wang.yeting.sql.ast.statement.SQLSelectItem(name));
                        } else if (from.getParent() instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource
                                && from instanceof wang.yeting.sql.ast.statement.SQLExprTableSource
                                && expr instanceof wang.yeting.sql.ast.SQLName) {
                            String tableName = expr.toString();
                            wang.yeting.sql.ast.expr.SQLPropertyExpr name = new wang.yeting.sql.ast.expr.SQLPropertyExpr(tableName, column.getName().getSimpleName());
                            name.setResolvedColumn(column);
                            columns.add(new wang.yeting.sql.ast.statement.SQLSelectItem(name));
                        } else {
                            wang.yeting.sql.ast.expr.SQLIdentifierExpr name = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) column.getName().clone();
                            name.setResolvedColumn(column);
                            columns.add(new wang.yeting.sql.ast.statement.SQLSelectItem(name));
                        }
                    }
                }
                return;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.statement.SQLTableSource resolvedTableSource = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr).getResolvedTableSource();
                if (resolvedTableSource instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) {
                    wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry = (wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) resolvedTableSource;
                    wang.yeting.sql.ast.statement.SQLSelect select = ((wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) resolvedTableSource).getSubQuery();
                    wang.yeting.sql.ast.statement.SQLSelectQueryBlock firstQueryBlock = select.getFirstQueryBlock();
                    if (firstQueryBlock != null) {
                        for (wang.yeting.sql.ast.statement.SQLSelectItem item : firstQueryBlock.getSelectList()) {
                            String itemAlias = item.computeAlias();
                            if (itemAlias != null) {
                                wang.yeting.sql.ast.expr.SQLIdentifierExpr columnExpr = new wang.yeting.sql.ast.expr.SQLIdentifierExpr(itemAlias);
                                columnExpr.setResolvedColumn(item);
                                columns.add(
                                        new wang.yeting.sql.ast.statement.SQLSelectItem(columnExpr));
                            }
                        }
                    }
                }
            }

        } else if (from instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
            wang.yeting.sql.ast.statement.SQLJoinTableSource join = (wang.yeting.sql.ast.statement.SQLJoinTableSource) from;
            extractColumns(visitor, join.getLeft(), ownerName, columns);
            extractColumns(visitor, join.getRight(), ownerName, columns);
        } else if (from instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock subQuery = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) from).getSelect().getQueryBlock();
            if (subQuery == null) {
                return;
            }
            final List<wang.yeting.sql.ast.statement.SQLSelectItem> subSelectList = subQuery.getSelectList();
            for (wang.yeting.sql.ast.statement.SQLSelectItem subSelectItem : subSelectList) {
                if (subSelectItem.getAlias() != null) {
                    continue;
                }

                if (!(subSelectItem.getExpr() instanceof wang.yeting.sql.ast.SQLName)) {
                    return; // skip
                }
            }

            for (wang.yeting.sql.ast.statement.SQLSelectItem subSelectItem : subSelectList) {
                String alias = subSelectItem.computeAlias();
                columns.add(new wang.yeting.sql.ast.statement.SQLSelectItem(new wang.yeting.sql.ast.expr.SQLIdentifierExpr(alias)));
            }
        } else if (from instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock firstQueryBlock = ((wang.yeting.sql.ast.statement.SQLUnionQueryTableSource) from).getUnion().getFirstQueryBlock();
            if (firstQueryBlock == null) {
                return;
            }

            final List<wang.yeting.sql.ast.statement.SQLSelectItem> subSelectList = firstQueryBlock.getSelectList();
            for (wang.yeting.sql.ast.statement.SQLSelectItem subSelectItem : subSelectList) {
                if (subSelectItem.getAlias() != null) {
                    continue;
                }

                if (!(subSelectItem.getExpr() instanceof wang.yeting.sql.ast.SQLName)) {
                    return; // skip
                }
            }

            for (wang.yeting.sql.ast.statement.SQLSelectItem subSelectItem : subSelectList) {
                String alias = subSelectItem.computeAlias();
                columns.add(new wang.yeting.sql.ast.statement.SQLSelectItem(new wang.yeting.sql.ast.expr.SQLIdentifierExpr(alias)));
            }
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.expr.SQLAllColumnExpr x) {
        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();

        if (tableSource == null) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = null;
            for (wang.yeting.sql.ast.SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
                if (parent instanceof wang.yeting.sql.ast.statement.SQLTableSource) {
                    return;
                }
                if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                    queryBlock = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) parent;
                    break;
                }
            }

            if (queryBlock == null) {
                return;
            }

            wang.yeting.sql.ast.statement.SQLTableSource from = queryBlock.getFrom();
            if (from == null || from instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
                return;
            }

            x.setResolvedTableSource(from);
            tableSource = from;
        }

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.statement.SQLTableSource resolvedTableSource = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr).getResolvedTableSource();
                if (resolvedTableSource != null) {
                    x.setResolvedTableSource(resolvedTableSource);
                }
            }
        }
    }

    static void resolve(SchemaResolveVisitor v, wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {
        wang.yeting.sql.ast.SQLExpr owner = x.getOwner();
        if (owner != null) {
            resolveExpr(v, owner);
        }

        for (wang.yeting.sql.ast.SQLExpr arg : x.getArguments()) {
            resolveExpr(v, arg);
        }

        wang.yeting.sql.ast.SQLExpr from = x.getFrom();
        if (from != null) {
            resolveExpr(v, from);
        }

        wang.yeting.sql.ast.SQLExpr using = x.getUsing();
        if (using != null) {
            resolveExpr(v, using);
        }

        wang.yeting.sql.ast.SQLExpr _for = x.getFor();
        if (_for != null) {
            resolveExpr(v, _for);
        }

        long nameHash = x.methodNameHashCode64();
        SchemaRepository repository = v.getRepository();
        if (repository != null) {
            wang.yeting.sql.ast.SQLDataType dataType = repository.findFuntionReturnType(nameHash);
            if (dataType != null) {
                x.setResolvedReturnDataType(dataType);
            }
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLSelect x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = x.getWithSubQuery();
        if (with != null) {
            visitor.visit(with);
        }

        wang.yeting.sql.ast.statement.SQLSelectQuery query = x.getQuery();
        if (query != null) {
            if (query instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                visitor.visit((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) query);
            } else {
                query.accept(visitor);
            }
        }

        wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = x.getFirstQueryBlock();

        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem orderByItem : orderBy.getItems()) {
                wang.yeting.sql.ast.SQLExpr orderByItemExpr = orderByItem.getExpr();

                if (orderByItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    wang.yeting.sql.ast.expr.SQLIdentifierExpr orderByItemIdentExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) orderByItemExpr;
                    long hash = orderByItemIdentExpr.nameHashCode64();

                    wang.yeting.sql.ast.statement.SQLSelectItem selectItem = null;
                    if (queryBlock != null) {
                        selectItem = queryBlock.findSelectItem(hash);
                    }

                    if (selectItem != null) {
                        orderByItem.setResolvedSelectItem(selectItem);

                        wang.yeting.sql.ast.SQLExpr selectItemExpr = selectItem.getExpr();
                        if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                            orderByItemIdentExpr.setResolvedTableSource(((wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr).getResolvedTableSource());
                            orderByItemIdentExpr.setResolvedColumn(((wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr).getResolvedColumn());
                        } else if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                            orderByItemIdentExpr.setResolvedTableSource(((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr).getResolvedTableSource());
                            orderByItemIdentExpr.setResolvedColumn(((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr).getResolvedColumn());
                        }
                        continue;
                    }
                }

                orderByItemExpr.accept(visitor);
            }
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLWithSubqueryClause x) {
        List<wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry> entries = x.getEntries();
        final SchemaResolveVisitor.Context context = visitor.getContext();
        for (wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry : entries) {
            wang.yeting.sql.ast.statement.SQLSelect query = entry.getSubQuery();
            if (query != null) {
                visitor.visit(query);

                final long alias_hash = entry.aliasHashCode64();
                if (context != null && alias_hash != 0) {
                    context.addTableSource(alias_hash, entry);
                }
            } else {
                entry.getReturningStatement().accept(visitor);
            }
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLExprTableSource x) {
        wang.yeting.sql.ast.SQLExpr expr = x.getExpr();

        wang.yeting.sql.ast.SQLExpr annFeature = null;
        if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
            wang.yeting.sql.ast.expr.SQLMethodInvokeExpr func = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
            if (func.methodNameHashCode64() == FnvHash.Constants.ANN) {
                expr = func.getArguments().get(0);

                annFeature = func.getArguments().get(1);
                if (annFeature instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) annFeature).setResolvedTableSource(x);
                } else if (annFeature instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    ((wang.yeting.sql.ast.expr.SQLPropertyExpr) annFeature).setResolvedTableSource(x);
                }
            }
        }

        if (expr instanceof wang.yeting.sql.ast.SQLName) {
            if (x.getSchemaObject() != null) {
                return;
            }

            wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = null;

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                wang.yeting.sql.ast.SQLExpr owner = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) expr).getOwner();
                if (owner instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) owner;
                }
            }

            if (identifierExpr != null) {
                checkParameter(visitor, identifierExpr);

                wang.yeting.sql.ast.statement.SQLTableSource tableSource = unwrapAlias(visitor.getContext(), null, identifierExpr.nameHashCode64());
                if (tableSource == null && x.getParent() instanceof HiveMultiInsertStatement) {
                    wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = ((HiveMultiInsertStatement) x.getParent()).getWith();
                    if (with != null) {
                        wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry = with.findEntry(identifierExpr.nameHashCode64());
                        tableSource = entry;
                    }
                }

                if (tableSource != null) {
                    identifierExpr.setResolvedTableSource(tableSource);
                    return;
                }
            }

            SchemaRepository repository = visitor.getRepository();
            if (repository != null) {
                SchemaObject table = repository.findTable((wang.yeting.sql.ast.SQLName) expr);
                if (table != null) {
                    x.setSchemaObject(table);

                    if (annFeature != null) {
                        if (annFeature instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                            wang.yeting.sql.ast.expr.SQLIdentifierExpr identExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) annFeature;
                            wang.yeting.sql.ast.statement.SQLColumnDefinition column = table.findColumn(identExpr.nameHashCode64());
                            if (column != null) {
                                identExpr.setResolvedColumn(column);
                            }
                        }
                    }
                    return;
                }

                SchemaObject view = repository.findView((wang.yeting.sql.ast.SQLName) expr);
                if (view != null) {
                    x.setSchemaObject(view);
                    return;
                }
            }
            return;
        }

        if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
            visitor.visit((wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr);
            return;
        }

        if (expr instanceof wang.yeting.sql.ast.expr.SQLQueryExpr) {
            wang.yeting.sql.ast.statement.SQLSelect select =
                    ((wang.yeting.sql.ast.expr.SQLQueryExpr) expr)
                            .getSubQuery();

            visitor.visit(select);

            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            if (queryBlock != null && annFeature instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr identExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) annFeature;
                wang.yeting.sql.ast.SQLObject columnDef = queryBlock.resolveColum(identExpr.nameHashCode64());
                if (columnDef instanceof wang.yeting.sql.ast.statement.SQLColumnDefinition) {
                    identExpr.setResolvedColumn((wang.yeting.sql.ast.statement.SQLColumnDefinition) columnDef);
                } else if (columnDef instanceof wang.yeting.sql.ast.statement.SQLSelectItem) {
                    identExpr.setResolvedColumn((wang.yeting.sql.ast.statement.SQLSelectItem) columnDef);
                }
            }
            //if (queryBlock.findColumn())
            return;
        }

        expr.accept(visitor);
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getTableSource();
        ctx.setTableSource(tableSource);

        for (wang.yeting.sql.ast.statement.SQLAlterTableItem item : x.getItems()) {
            item.accept(visitor);
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLMergeStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLTableSource into = x.getInto();
        if (into instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            ctx.setTableSource(into);
        } else {
            into.accept(visitor);
        }

        wang.yeting.sql.ast.statement.SQLTableSource using = x.getUsing();
        if (using != null) {
            using.accept(visitor);
            ctx.setFrom(using);
        }

        wang.yeting.sql.ast.SQLExpr on = x.getOn();
        if (on != null) {
            on.accept(visitor);
        }

        wang.yeting.sql.ast.statement.SQLMergeStatement.MergeUpdateClause updateClause = x.getUpdateClause();
        if (updateClause != null) {
            for (wang.yeting.sql.ast.statement.SQLUpdateSetItem item : updateClause.getItems()) {
                wang.yeting.sql.ast.SQLExpr column = item.getColumn();

                if (column instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) column).setResolvedTableSource(into);
                } else if (column instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    ((wang.yeting.sql.ast.expr.SQLPropertyExpr) column).setResolvedTableSource(into);
                } else {
                    column.accept(visitor);
                }

                wang.yeting.sql.ast.SQLExpr value = item.getValue();
                if (value != null) {
                    value.accept(visitor);
                }
            }

            wang.yeting.sql.ast.SQLExpr where = updateClause.getWhere();
            if (where != null) {
                where.accept(visitor);
            }

            wang.yeting.sql.ast.SQLExpr deleteWhere = updateClause.getDeleteWhere();
            if (deleteWhere != null) {
                deleteWhere.accept(visitor);
            }
        }

        wang.yeting.sql.ast.statement.SQLMergeStatement.MergeInsertClause insertClause = x.getInsertClause();
        if (insertClause != null) {
            for (wang.yeting.sql.ast.SQLExpr column : insertClause.getColumns()) {
                if (column instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) column).setResolvedTableSource(into);
                } else if (column instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    ((wang.yeting.sql.ast.expr.SQLPropertyExpr) column).setResolvedTableSource(into);
                }
                column.accept(visitor);
            }
            for (wang.yeting.sql.ast.SQLExpr value : insertClause.getValues()) {
                value.accept(visitor);
            }
            wang.yeting.sql.ast.SQLExpr where = insertClause.getWhere();
            if (where != null) {
                where.accept(visitor);
            }
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        {
            wang.yeting.sql.ast.SQLDeclareItem declareItem = new wang.yeting.sql.ast.SQLDeclareItem(x.getName().clone(), null);
            declareItem.setResolvedObject(x);

            SchemaResolveVisitor.Context parentCtx = visitor.getContext();
            if (parentCtx != null) {
                parentCtx.declare(declareItem);
            } else {
                ctx.declare(declareItem);
            }
        }

        for (wang.yeting.sql.ast.SQLParameter parameter : x.getParameters()) {
            parameter.accept(visitor);
        }

        wang.yeting.sql.ast.SQLStatement block = x.getBlock();
        if (block != null) {
            block.accept(visitor);
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLCreateProcedureStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        {
            wang.yeting.sql.ast.SQLDeclareItem declareItem = new wang.yeting.sql.ast.SQLDeclareItem(x.getName().clone(), null);
            declareItem.setResolvedObject(x);


            SchemaResolveVisitor.Context parentCtx = visitor.getContext();
            if (parentCtx != null) {
                parentCtx.declare(declareItem);
            } else {
                ctx.declare(declareItem);
            }
        }

        for (wang.yeting.sql.ast.SQLParameter parameter : x.getParameters()) {
            parameter.accept(visitor);
        }

        wang.yeting.sql.ast.SQLStatement block = x.getBlock();
        if (block != null) {
            block.accept(visitor);
        }

        visitor.popContext();
    }

    static boolean resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLIfStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.SQLExpr condition = x.getCondition();
        if (condition != null) {
            condition.accept(visitor);
        }

        for (wang.yeting.sql.ast.SQLStatement stmt : x.getStatements()) {
            stmt.accept(visitor);
        }

        for (wang.yeting.sql.ast.statement.SQLIfStatement.ElseIf elseIf : x.getElseIfList()) {
            elseIf.accept(visitor);
        }

        wang.yeting.sql.ast.statement.SQLIfStatement.Else e = x.getElseItem();
        if (e != null) {
            e.accept(visitor);
        }

        visitor.popContext();
        return false;
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLBlockStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        for (wang.yeting.sql.ast.SQLParameter parameter : x.getParameters()) {
            visitor.visit(parameter);
        }

        for (wang.yeting.sql.ast.SQLStatement stmt : x.getStatementList()) {
            stmt.accept(visitor);
        }

        wang.yeting.sql.ast.SQLStatement exception = x.getException();
        if (exception != null) {
            exception.accept(visitor);
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.SQLParameter x) {
        wang.yeting.sql.ast.SQLName name = x.getName();
        if (name instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) name).setResolvedParameter(x);
        }

        wang.yeting.sql.ast.SQLExpr expr = x.getDefaultValue();

        SchemaResolveVisitor.Context ctx = null;
        if (expr != null) {
            if (expr instanceof wang.yeting.sql.ast.expr.SQLQueryExpr) {
                ctx = visitor.createContext(x);

                wang.yeting.sql.ast.statement.SQLSubqueryTableSource tableSource = new wang.yeting.sql.ast.statement.SQLSubqueryTableSource(((wang.yeting.sql.ast.expr.SQLQueryExpr) expr).getSubQuery());
                tableSource.setParent(x);
                tableSource.setAlias(x.getName().getSimpleName());

                ctx.setTableSource(tableSource);
            }

            expr.accept(visitor);
        }

        if (ctx != null) {
            visitor.popContext();
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.SQLDeclareItem x) {
        SchemaResolveVisitor.Context ctx = visitor.getContext();
        if (ctx != null) {
            ctx.declare(x);
        }

        wang.yeting.sql.ast.SQLName name = x.getName();
        if (name instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) name).setResolvedDeclareItem(x);
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.SQLOver x) {
        wang.yeting.sql.ast.SQLName of = x.getOf();
        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        List<wang.yeting.sql.ast.SQLExpr> partitionBy = x.getPartitionBy();


        if (of == null // skip if of is not null
                && orderBy != null) {
            orderBy.accept(visitor);
        }

        if (partitionBy != null) {
            for (wang.yeting.sql.ast.SQLExpr expr : partitionBy) {
                expr.accept(visitor);
            }
        }
    }

    private static boolean checkParameter(SchemaResolveVisitor visitor, wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        if (x.getResolvedParameter() != null) {
            return true;
        }

        SchemaResolveVisitor.Context ctx = visitor.getContext();
        if (ctx == null) {
            return false;
        }

        long hash = x.hashCode64();
        for (SchemaResolveVisitor.Context parentCtx = ctx;
             parentCtx != null;
             parentCtx = parentCtx.parent) {

            if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLBlockStatement) {
                wang.yeting.sql.ast.statement.SQLBlockStatement block = (wang.yeting.sql.ast.statement.SQLBlockStatement) parentCtx.object;
                wang.yeting.sql.ast.SQLParameter parameter = block.findParameter(hash);
                if (parameter != null) {
                    x.setResolvedParameter(parameter);
                    return true;
                }
            }

            if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLCreateProcedureStatement) {
                wang.yeting.sql.ast.statement.SQLCreateProcedureStatement createProc = (wang.yeting.sql.ast.statement.SQLCreateProcedureStatement) parentCtx.object;
                wang.yeting.sql.ast.SQLParameter parameter = createProc.findParameter(hash);
                if (parameter != null) {
                    x.setResolvedParameter(parameter);
                    return true;
                }
            }

            if (parentCtx.object instanceof wang.yeting.sql.ast.statement.SQLSelect) {
                wang.yeting.sql.ast.statement.SQLSelect select = (wang.yeting.sql.ast.statement.SQLSelect) parentCtx.object;
                wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = select.getWithSubQuery();
                if (with != null) {
                    wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry entry = with.findEntry(hash);
                    if (entry != null) {
                        x.setResolvedTableSource(entry);
                        return true;
                    }
                }

                SchemaRepository repo = visitor.getRepository();
                if (repo != null) {
                    SchemaObject view = repo.findView(x);
                    if (view != null && view.getStatement() instanceof wang.yeting.sql.ast.statement.SQLCreateViewStatement) {
                        x.setResolvedOwnerObject(view.getStatement());
                    }
                }
            }

            wang.yeting.sql.ast.SQLDeclareItem declareItem = parentCtx.findDeclare(hash);
            if (declareItem != null) {
                x.setResolvedDeclareItem(declareItem);
                break;
            }
        }
        return false;
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLReplaceStatement x) {
        SchemaResolveVisitor.Context ctx = visitor.createContext(x);

        wang.yeting.sql.ast.statement.SQLExprTableSource tableSource = x.getTableSource();
        ctx.setTableSource(tableSource);
        visitor.visit(tableSource);

        for (wang.yeting.sql.ast.SQLExpr column : x.getColumns()) {
            column.accept(visitor);
        }

        wang.yeting.sql.ast.expr.SQLQueryExpr queryExpr = x.getQuery();
        if (queryExpr != null) {
            visitor.visit(queryExpr.getSubQuery());
        }

        visitor.popContext();
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLFetchStatement x) {
        resolveExpr(visitor, x.getCursorName());
        for (wang.yeting.sql.ast.SQLExpr expr : x.getInto()) {
            resolveExpr(visitor, expr);
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLForeignKeyConstraint x) {
        SchemaRepository repository = visitor.getRepository();
        wang.yeting.sql.ast.SQLObject parent = x.getParent();

        if (parent instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
            wang.yeting.sql.ast.statement.SQLCreateTableStatement createTableStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) parent;
            wang.yeting.sql.ast.statement.SQLTableSource table = createTableStmt.getTableSource();
            for (wang.yeting.sql.ast.SQLName item : x.getReferencingColumns()) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr columnName = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) item;
                columnName.setResolvedTableSource(table);

                wang.yeting.sql.ast.statement.SQLColumnDefinition column = createTableStmt.findColumn(columnName.nameHashCode64());
                if (column != null) {
                    columnName.setResolvedColumn(column);
                }
            }
        } else if (parent instanceof wang.yeting.sql.ast.statement.SQLAlterTableAddConstraint) {
            wang.yeting.sql.ast.statement.SQLAlterTableStatement stmt = (wang.yeting.sql.ast.statement.SQLAlterTableStatement) parent.getParent();
            wang.yeting.sql.ast.statement.SQLTableSource table = stmt.getTableSource();
            for (wang.yeting.sql.ast.SQLName item : x.getReferencingColumns()) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr columnName = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) item;
                columnName.setResolvedTableSource(table);
            }
        }


        if (repository == null) {
            return;
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getReferencedTable();
        for (wang.yeting.sql.ast.SQLName item : x.getReferencedColumns()) {
            wang.yeting.sql.ast.expr.SQLIdentifierExpr columnName = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) item;
            columnName.setResolvedTableSource(table);
        }

        wang.yeting.sql.ast.SQLName tableName = table.getName();

        SchemaObject tableObject = repository.findTable(tableName);
        if (tableObject == null) {
            return;
        }

        wang.yeting.sql.ast.SQLStatement tableStmt = tableObject.getStatement();
        if (tableStmt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
            wang.yeting.sql.ast.statement.SQLCreateTableStatement refCreateTableStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) tableStmt;
            for (wang.yeting.sql.ast.SQLName item : x.getReferencedColumns()) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr columnName = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) item;
                wang.yeting.sql.ast.statement.SQLColumnDefinition column = refCreateTableStmt.findColumn(columnName.nameHashCode64());
                if (column != null) {
                    columnName.setResolvedColumn(column);
                }
            }
        }
    }

    static void resolve(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
        x.getSubQuery()
                .accept(visitor);
    }

    // for performance
    static void resolveExpr(SchemaResolveVisitor visitor, wang.yeting.sql.ast.SQLExpr x) {
        if (x == null) {
            return;
        }

        Class<?> clazz = x.getClass();
        if (clazz == wang.yeting.sql.ast.expr.SQLIdentifierExpr.class) {
            visitor.visit((wang.yeting.sql.ast.expr.SQLIdentifierExpr) x);
            return;
        } else if (clazz == wang.yeting.sql.ast.expr.SQLIntegerExpr.class || clazz == wang.yeting.sql.ast.expr.SQLCharExpr.class) {
            // skip
            return;
        }

        x.accept(visitor);
    }

    static void resolveUnion(SchemaResolveVisitor visitor, wang.yeting.sql.ast.statement.SQLUnionQuery x) {
        wang.yeting.sql.ast.statement.SQLUnionOperator operator = x.getOperator();
        List<wang.yeting.sql.ast.statement.SQLSelectQuery> relations = x.getRelations();
        if (relations.size() > 2) {
            for (wang.yeting.sql.ast.statement.SQLSelectQuery relation : relations) {
                relation.accept(visitor);
            }
            return;
        }
        wang.yeting.sql.ast.statement.SQLSelectQuery left = x.getLeft();
        wang.yeting.sql.ast.statement.SQLSelectQuery right = x.getRight();

        boolean bracket = x.isParenthesized() && !(x.getParent() instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource);

        if ((!bracket)
                && left instanceof wang.yeting.sql.ast.statement.SQLUnionQuery
                && ((wang.yeting.sql.ast.statement.SQLUnionQuery) left).getOperator() == operator
                && !right.isParenthesized()
                && x.getOrderBy() == null) {

            wang.yeting.sql.ast.statement.SQLUnionQuery leftUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) left;

            List<wang.yeting.sql.ast.statement.SQLSelectQuery> rights = new ArrayList<wang.yeting.sql.ast.statement.SQLSelectQuery>();
            rights.add(right);

            if (leftUnion.getRelations().size() > 2) {
                rights.addAll(leftUnion.getRelations());
            } else {
                for (; ; ) {
                    wang.yeting.sql.ast.statement.SQLSelectQuery leftLeft = leftUnion.getLeft();
                    wang.yeting.sql.ast.statement.SQLSelectQuery leftRight = leftUnion.getRight();

                    if ((!leftUnion.isParenthesized())
                            && leftUnion.getOrderBy() == null
                            && (!leftLeft.isParenthesized())
                            && (!leftRight.isParenthesized())
                            && leftLeft instanceof wang.yeting.sql.ast.statement.SQLUnionQuery
                            && ((wang.yeting.sql.ast.statement.SQLUnionQuery) leftLeft).getOperator() == operator) {
                        rights.add(leftRight);
                        leftUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) leftLeft;
                        continue;
                    } else {
                        rights.add(leftRight);
                        rights.add(leftLeft);
                    }
                    break;
                }
            }

            for (int i = rights.size() - 1; i >= 0; i--) {
                wang.yeting.sql.ast.statement.SQLSelectQuery item = rights.get(i);
                item.accept(visitor);
            }
            return;
        }

        if (left != null) {
            left.accept(visitor);
        }

        if (right != null) {
            right.accept(visitor);
        }
    }

    static class MySqlResolveVisitor extends MySqlASTVisitorAdapter implements SchemaResolveVisitor {
        private SchemaRepository repository;
        private int options;
        private Context context;

        public MySqlResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(MySqlRepeatStatement x) {
            return true;
        }

        public boolean visit(MySqlDeclareStatement x) {
            for (wang.yeting.sql.ast.SQLDeclareItem declareItem : x.getVarList()) {
                visit(declareItem);
            }
            return false;
        }

        public boolean visit(MySqlCursorDeclareStatement x) {
            return true;
        }

        public boolean visit(MysqlForeignKey x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(MySqlSelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(MySqlCreateTableStatement x) {
            resolve(this, x);
            wang.yeting.sql.ast.statement.SQLExprTableSource like = x.getLike();
            if (like != null) {
                like.accept(this);
            }
            return false;
        }

        public boolean visit(MySqlUpdateStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(MySqlDeleteStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(MySqlInsertStatement x) {
            resolve(this, x);
            return false;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class DB2ResolveVisitor extends DB2ASTVisitorAdapter implements SchemaResolveVisitor {
        private SchemaRepository repository;
        private int options;
        private Context context;

        public DB2ResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(DB2SelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            long hash64 = x.hashCode64();
            if (hash64 == FnvHash.Constants.CURRENT_DATE || hash64 == DB2Object.Constants.CURRENT_TIME) {
                return false;
            }

            resolveIdent(this, x);
            return true;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        @Override
        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class OracleResolveVisitor extends OracleASTVisitorAdapter implements SchemaResolveVisitor {
        private SchemaRepository repository;
        private int options;
        private Context context;

        public OracleResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(OracleCreatePackageStatement x) {
            Context ctx = createContext(x);

            for (wang.yeting.sql.ast.SQLStatement stmt : x.getStatements()) {
                stmt.accept(this);
            }

            popContext();
            return false;
        }

        public boolean visit(OracleForStatement x) {
            Context ctx = createContext(x);

            wang.yeting.sql.ast.SQLName index = x.getIndex();
            wang.yeting.sql.ast.SQLExpr range = x.getRange();

            if (index != null) {
                wang.yeting.sql.ast.SQLDeclareItem declareItem = new wang.yeting.sql.ast.SQLDeclareItem(index, null);
                declareItem.setParent(x);

                if (index instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) index).setResolvedDeclareItem(declareItem);
                }
                declareItem.setResolvedObject(range);
                ctx.declare(declareItem);
                if (range instanceof wang.yeting.sql.ast.expr.SQLQueryExpr) {
                    wang.yeting.sql.ast.statement.SQLSelect select = ((wang.yeting.sql.ast.expr.SQLQueryExpr) range).getSubQuery();
                    wang.yeting.sql.ast.statement.SQLSubqueryTableSource tableSource = new wang.yeting.sql.ast.statement.SQLSubqueryTableSource(select);
                    declareItem.setResolvedObject(tableSource);
                }

                index.accept(this);
            }


            if (range != null) {
                range.accept(this);
            }

            for (wang.yeting.sql.ast.SQLStatement stmt : x.getStatements()) {
                stmt.accept(this);
            }

            popContext();
            return false;
        }

        public boolean visit(OracleForeignKey x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(OracleSelectTableReference x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(OracleSelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            if (x.nameHashCode64() == FnvHash.Constants.ROWNUM) {
                return false;
            }

            resolveIdent(this, x);
            return true;
        }

        public boolean visit(OracleCreateTableStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(OracleUpdateStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(OracleDeleteStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(OracleMultiInsertStatement x) {
            Context ctx = createContext(x);

            wang.yeting.sql.ast.statement.SQLSelect select = x.getSubQuery();
            visit(select);

            OracleSelectSubqueryTableSource tableSource = new OracleSelectSubqueryTableSource(select);
            tableSource.setParent(x);
            ctx.setTableSource(tableSource);

            for (OracleMultiInsertStatement.Entry entry : x.getEntries()) {
                entry.accept(this);
            }

            popContext();
            return false;
        }

        public boolean visit(OracleMultiInsertStatement.InsertIntoClause x) {
            for (wang.yeting.sql.ast.SQLExpr column : x.getColumns()) {
                if (column instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    wang.yeting.sql.ast.expr.SQLIdentifierExpr identColumn = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) column;
                    identColumn.setResolvedTableSource(x.getTableSource());
                }
            }
            return true;
        }

        public boolean visit(OracleInsertStatement x) {
            resolve(this, x);
            return false;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class OdpsResolveVisitor extends OdpsASTVisitorAdapter implements SchemaResolveVisitor {
        private int options;
        private SchemaRepository repository;
        private Context context;

        public OdpsResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(OdpsSelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(OdpsCreateTableStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(HiveInsert x) {
            Context ctx = createContext(x);

            wang.yeting.sql.ast.statement.SQLExprTableSource tableSource = x.getTableSource();
            if (tableSource != null) {
                ctx.setTableSource(x.getTableSource());
                visit(tableSource);
            }

            List<wang.yeting.sql.ast.statement.SQLAssignItem> partitions = x.getPartitions();
            if (partitions != null) {
                for (wang.yeting.sql.ast.statement.SQLAssignItem item : partitions) {
                    item.accept(this);
                }
            }

            wang.yeting.sql.ast.statement.SQLSelect select = x.getQuery();
            if (select != null) {
                visit(select);
            }

            popContext();
            return false;
        }

        public boolean visit(HiveInsertStatement x) {
            resolve(this, x);
            return false;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class HiveResolveVisitor extends HiveASTVisitorAdapter implements SchemaResolveVisitor {
        private int options;
        private SchemaRepository repository;
        private Context context;

        public HiveResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(OdpsSelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(HiveCreateTableStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(HiveInsert x) {
            Context ctx = createContext(x);

            wang.yeting.sql.ast.statement.SQLExprTableSource tableSource = x.getTableSource();
            if (tableSource != null) {
                ctx.setTableSource(x.getTableSource());
                visit(tableSource);
            }

            List<wang.yeting.sql.ast.statement.SQLAssignItem> partitions = x.getPartitions();
            if (partitions != null) {
                for (wang.yeting.sql.ast.statement.SQLAssignItem item : partitions) {
                    item.accept(this);
                }
            }

            wang.yeting.sql.ast.statement.SQLSelect select = x.getQuery();
            if (select != null) {
                visit(select);
            }

            popContext();
            return false;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class PGResolveVisitor extends PGASTVisitorAdapter implements SchemaResolveVisitor {
        private int options;
        private SchemaRepository repository;
        private Context context;

        public PGResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(PGSelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(PGFunctionTableSource x) {
            for (wang.yeting.sql.ast.SQLParameter parameter : x.getParameters()) {
                wang.yeting.sql.ast.SQLName name = parameter.getName();
                if (name instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    wang.yeting.sql.ast.expr.SQLIdentifierExpr identName = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) name;
                    identName.setResolvedTableSource(x);
                }
            }

            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            if (PGUtils.isPseudoColumn(x.nameHashCode64())) {
                return false;
            }

            resolveIdent(this, x);
            return true;
        }

        public boolean visit(PGUpdateStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(PGDeleteStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(PGSelectStatement x) {
            createContext(x);
            visit(x.getSelect());
            popContext();
            return false;
        }

        public boolean visit(PGInsertStatement x) {
            resolve(this, x);
            return false;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class SQLServerResolveVisitor extends SQLServerASTVisitorAdapter implements SchemaResolveVisitor {
        private int options;
        private SchemaRepository repository;
        private Context context;

        public SQLServerResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(SQLServerSelectQueryBlock x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        public boolean visit(SQLServerUpdateStatement x) {
            resolve(this, x);
            return false;
        }

        public boolean visit(SQLServerInsertStatement x) {
            resolve(this, x);
            return false;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }

    static class SQLResolveVisitor extends SQLASTVisitorAdapter implements SchemaResolveVisitor {
        private int options;
        private SchemaRepository repository;
        private Context context;

        public SQLResolveVisitor(SchemaRepository repository, int options) {
            this.repository = repository;
            this.options = options;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
            wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                resolveIdent(this, (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
                return false;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                resolve(this, (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
                return false;
            }

            return true;
        }

        @Override
        public boolean isEnabled(Option option) {
            return (options & option.mask) != 0;
        }

        public int getOptions() {
            return options;
        }

        @Override
        public Context getContext() {
            return context;
        }

        public Context createContext(wang.yeting.sql.ast.SQLObject object) {
            return this.context = new Context(object, context);
        }

        @Override
        public void popContext() {
            if (context != null) {
                context = context.parent;
            }
        }

        public SchemaRepository getRepository() {
            return repository;
        }
    }
}

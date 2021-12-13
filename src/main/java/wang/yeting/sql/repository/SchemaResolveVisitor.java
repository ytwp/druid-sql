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

import wang.yeting.sql.ast.SQLDeclareItem;
import wang.yeting.sql.ast.SQLObject;
import wang.yeting.sql.ast.SQLOver;
import wang.yeting.sql.ast.SQLParameter;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.HashMap;
import java.util.Map;

import static wang.yeting.sql.repository.SchemaResolveVisitorFactory.*;

/**
 * Created by wenshao on 03/08/2017.
 */
public interface SchemaResolveVisitor extends SQLASTVisitor {

    boolean isEnabled(Option option);

    int getOptions();

    SchemaRepository getRepository();

    Context getContext();

    Context createContext(SQLObject object);

    void popContext();

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectStatement x) {
        resolve(this, x.getSelect());
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelect x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLWithSubqueryClause x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLIfStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLExprTableSource x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLSelectQueryBlock x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLForeignKeyImpl x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        resolveIdent(this, x);
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLAllColumnExpr x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUpdateStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLDeleteStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLInsertStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(SQLParameter x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(SQLDeclareItem x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(SQLOver x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLUnionQuery x) {
        resolveUnion(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLMergeStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLCreateProcedureStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLBlockStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLReplaceStatement x) {
        resolve(this, x);
        return false;
    }

    default boolean visit(wang.yeting.sql.ast.expr.SQLCastExpr x) {
        x.getExpr()
                .accept(this);
        return true;
    }

    default boolean visit(wang.yeting.sql.ast.statement.SQLFetchStatement x) {
        resolve(this, x);
        return false;
    }

    public static enum Option {
        ResolveAllColumn,
        ResolveIdentifierAlias,
        CheckColumnAmbiguous;

        public final int mask;

        private Option() {
            mask = (1 << ordinal());
        }

        public static int of(Option... options) {
            if (options == null) {
                return 0;
            }

            int value = 0;

            for (Option option : options) {
                value |= option.mask;
            }

            return value;
        }
    }

    static class Context {
        public final Context parent;
        public final SQLObject object;
        public final int level;
        protected Map<Long, SQLDeclareItem> declares;
        private wang.yeting.sql.ast.statement.SQLTableSource tableSource;
        private wang.yeting.sql.ast.statement.SQLTableSource from;
        private Map<Long, wang.yeting.sql.ast.statement.SQLTableSource> tableSourceMap;

        public Context(SQLObject object, Context parent) {
            this.object = object;
            this.parent = parent;
            this.level = parent == null
                    ? 0
                    : parent.level + 1;
        }

        public wang.yeting.sql.ast.statement.SQLTableSource getFrom() {
            return from;
        }

        public void setFrom(wang.yeting.sql.ast.statement.SQLTableSource from) {
            this.from = from;
        }

        public wang.yeting.sql.ast.statement.SQLTableSource getTableSource() {
            return tableSource;
        }

        public void setTableSource(wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
            this.tableSource = tableSource;
        }

        public void addTableSource(long alias_hash, wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
            if (tableSourceMap == null) {
                tableSourceMap = new HashMap<Long, wang.yeting.sql.ast.statement.SQLTableSource>();
            }

            tableSourceMap.put(alias_hash, tableSource);
        }

        protected void declare(SQLDeclareItem x) {
            if (declares == null) {
                declares = new HashMap<Long, SQLDeclareItem>();
            }
            declares.put(x.getName().nameHashCode64(), x);
        }

        protected SQLDeclareItem findDeclare(long nameHash) {
            if (declares == null) {
                return null;
            }
            return declares.get(nameHash);
        }

        protected wang.yeting.sql.ast.statement.SQLTableSource findTableSource(long nameHash) {
            wang.yeting.sql.ast.statement.SQLTableSource table = null;
            if (tableSourceMap != null) {
                table = tableSourceMap.get(nameHash);
            }

            return table;
        }

        protected wang.yeting.sql.ast.statement.SQLTableSource findTableSourceRecursive(long nameHash) {
            for (Context ctx = this; ctx != null; ctx = ctx.parent) {
                if (ctx.tableSourceMap != null) {
                    wang.yeting.sql.ast.statement.SQLTableSource table = ctx.tableSourceMap.get(nameHash);
                    if (table != null) {
                        return table;
                    }
                }
            }

            return null;
        }
    }
}

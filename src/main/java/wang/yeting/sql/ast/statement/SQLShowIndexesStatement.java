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
package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLCommentHint;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.ast.expr.SQLPropertyExpr;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.List;

public class SQLShowIndexesStatement extends SQLStatementImpl implements SQLShowStatement {
    private SQLExprTableSource table;
    private List<SQLCommentHint> hints;
    private SQLExpr where;
    private String type;

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLName table) {
        setTable(new SQLExprTableSource(table));
    }

    public void setTable(SQLExprTableSource table) {
        this.table = table;
    }

    public SQLName getDatabase() {
        SQLExpr expr = table.getExpr();
        if (expr instanceof SQLPropertyExpr) {
            return (SQLName) ((SQLPropertyExpr) expr).getOwner();
        }
        return null;
    }

    public void setDatabase(String database) {
        table.setSchema(database);
    }

    public SQLExpr getWhere() {
        return where;
    }

    public void setWhere(SQLExpr where) {
        this.where = where;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, table);
            acceptChild(visitor, where);
        }
        visitor.endVisit(this);
    }

    public List<SQLCommentHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLCommentHint> hints) {
        this.hints = hints;
    }
}

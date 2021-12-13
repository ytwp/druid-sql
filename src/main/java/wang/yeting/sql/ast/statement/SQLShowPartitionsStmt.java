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

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLShowPartitionsStmt extends SQLStatementImpl implements SQLShowStatement {

    private SQLExprTableSource tableSource;

    private List<SQLAssignItem> partition = new ArrayList<SQLAssignItem>();

    private SQLExpr where;

    public SQLShowPartitionsStmt() {
        super(DbType.odps);
    }

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }

    public void setTableSource(SQLExpr table) {
        if (table == null) {
            return;
        }

        setTableSource(new SQLExprTableSource(table));
    }

    public void setTableSource(SQLExprTableSource tableSource) {
        this.tableSource = tableSource;
    }

    public List<SQLAssignItem> getPartition() {
        return partition;
    }

    public SQLExpr getWhere() {
        return where;
    }

    public void setWhere(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.where = x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, partition);
            acceptChild(visitor, where);
        }
        visitor.endVisit(this);
    }
}

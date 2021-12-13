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
package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.statement.SQLAlterStatement;
import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.ast.statement.SQLPrivilegeItem;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class OdpsAddTableStatement extends OdpsStatementImpl implements SQLAlterStatement {

    protected final List<SQLPrivilegeItem> privileges = new ArrayList<SQLPrivilegeItem>();
    private final List<SQLAssignItem> partitions = new ArrayList<SQLAssignItem>();
    protected SQLExpr comment;
    protected boolean force;
    protected SQLName toPackage;
    private SQLExprTableSource table;

    public OdpsAddTableStatement() {
        super.dbType = DbType.odps;
    }


    @Override
    protected void accept0(OdpsASTVisitor visitor) {
        if (visitor.visit(this)) {
            this.acceptChild(visitor, table);
        }
        visitor.endVisit(this);
    }

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLExprTableSource table) {
        if (table != null) {
            table.setParent(table);
        }
        this.table = table;
    }

    public void setTable(SQLName table) {
        this.setTable(new SQLExprTableSource(table));
    }

    public SQLExpr getComment() {
        return comment;
    }

    public void setComment(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.comment = x;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public List<SQLAssignItem> getPartitions() {
        return partitions;
    }

    public SQLName getToPackage() {
        return toPackage;
    }

    public void setToPackage(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }

        this.toPackage = x;
    }

    public List<SQLPrivilegeItem> getPrivileges() {
        return privileges;
    }
}

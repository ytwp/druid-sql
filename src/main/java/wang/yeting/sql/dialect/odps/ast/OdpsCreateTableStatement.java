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
import wang.yeting.sql.ast.statement.SQLColumnDefinition;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateTableStatement;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class OdpsCreateTableStatement extends HiveCreateTableStatement {
    protected final List<SQLExpr> withSerdeproperties = new ArrayList<SQLExpr>();
    protected SQLExpr lifecycle;
    protected SQLExpr storedBy;

    public OdpsCreateTableStatement() {
        super(DbType.odps);
    }

    public SQLExprTableSource getLike() {
        return like;
    }

    public void setLike(SQLName like) {
        this.setLike(new SQLExprTableSource(like));
    }

    public void setLike(SQLExprTableSource like) {
        this.like = like;
    }

    public List<SQLColumnDefinition> getPartitionColumns() {
        return partitionColumns;
    }

    public void addPartitionColumn(SQLColumnDefinition column) {
        if (column != null) {
            column.setParent(this);
        }
        this.partitionColumns.add(column);
    }

    public SQLExpr getLifecycle() {
        return lifecycle;
    }

    public void setLifecycle(SQLExpr lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v instanceof OdpsASTVisitor) {
            accept0((OdpsASTVisitor) v);
            return;
        }

        super.accept0(v);
    }

    protected void accept0(OdpsASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v);
        }
        v.endVisit(this);
    }

    protected void acceptChild(SQLASTVisitor v) {
        super.acceptChild(v);

        acceptChild(v, withSerdeproperties);
        acceptChild(v, lifecycle);
        acceptChild(v, storedBy);
    }

    public SQLExpr getStoredBy() {
        return storedBy;
    }

    public void setStoredBy(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.storedBy = x;
    }

    public List<SQLExpr> getWithSerdeproperties() {
        return withSerdeproperties;
    }


}

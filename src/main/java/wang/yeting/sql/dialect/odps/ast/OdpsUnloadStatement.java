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
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.ast.statement.SQLExternalRecordFormat;
import wang.yeting.sql.ast.statement.SQLTableSource;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class OdpsUnloadStatement extends SQLStatementImpl {

    protected final List<SQLAssignItem> serdeProperties = new ArrayList<SQLAssignItem>();
    protected final List<SQLAssignItem> properties = new ArrayList<SQLAssignItem>();
    private final List<SQLAssignItem> partitions = new ArrayList<SQLAssignItem>();
    protected SQLExpr location;
    protected SQLExternalRecordFormat rowFormat;
    protected SQLExpr storedAs;
    private SQLTableSource from;

    public OdpsUnloadStatement() {
        super(DbType.odps);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        accept0((OdpsASTVisitor) visitor);
    }

    protected void accept0(OdpsASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, from);
            acceptChild(visitor, partitions);
            acceptChild(visitor, location);
            acceptChild(visitor, rowFormat);
            acceptChild(visitor, storedAs);
            acceptChild(visitor, properties);
        }
        visitor.endVisit(this);
    }

    public SQLTableSource getFrom() {
        return from;
    }

    public void setFrom(SQLName x) {
        setFrom(
                new SQLExprTableSource(x));
    }

    public void setFrom(SQLTableSource x) {
        if (x != null) {
            x.setParent(this);
        }
        this.from = x;
    }

    public List<SQLAssignItem> getPartitions() {
        return partitions;
    }

    public SQLExpr getLocation() {
        return location;
    }

    public void setLocation(SQLExpr location) {
        this.location = location;
    }

    public SQLExternalRecordFormat getRowFormat() {
        return rowFormat;
    }

    public void setRowFormat(SQLExternalRecordFormat x) {
        if (x != null) {
            x.setParent(this);
        }
        this.rowFormat = x;
    }

    public SQLExpr getStoredAs() {
        return storedAs;
    }

    public void setStoredAs(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.storedAs = x;
    }

    public List<SQLAssignItem> getSerdeProperties() {
        return serdeProperties;
    }

    public List<SQLAssignItem> getProperties() {
        return properties;
    }
}

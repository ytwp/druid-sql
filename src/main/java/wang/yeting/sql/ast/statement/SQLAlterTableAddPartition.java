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

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLObject;
import wang.yeting.sql.ast.SQLObjectImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLAlterTableAddPartition extends SQLObjectImpl implements SQLAlterTableItem {

    private final List<SQLObject> partitions = new ArrayList<SQLObject>(4);
    private boolean ifNotExists = false;
    private SQLExpr partitionCount;
    private SQLExpr location; // hive

    public List<SQLObject> getPartitions() {
        return partitions;
    }

    public void addPartition(SQLObject x) {
        if (x != null) {
            x.setParent(this);
        }
        this.partitions.add(x);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SQLExpr getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.partitionCount = x;
    }

    public SQLExpr getLocation() {
        return location;
    }

    public void setLocation(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.location = x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, partitions);
            acceptChild(visitor, partitionCount);
            acceptChild(visitor, location);
        }
        visitor.endVisit(this);
    }
}

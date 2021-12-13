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
import wang.yeting.sql.SQLUtils;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLObject;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.ast.expr.SQLCharExpr;
import wang.yeting.sql.ast.expr.SQLIdentifierExpr;
import wang.yeting.sql.ast.expr.SQLPropertyExpr;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLCreateTableGroupStatement extends SQLStatementImpl implements SQLCreateStatement {
    protected SQLName name;
    protected boolean ifNotExists = false;
    protected SQLExpr partitionNum;


    public SQLCreateTableGroupStatement() {
    }

    public SQLCreateTableGroupStatement(DbType dbType) {
        super(dbType);
    }

    public String getSchemaName() {
        if (name instanceof SQLPropertyExpr) {
            return SQLUtils.toMySqlString(((SQLPropertyExpr) name).getOwner());
        }
        return null;
    }

    public void setSchemaName(String name) {
        if (name != null) {
            this.name = new SQLPropertyExpr(name, getTableGroupName());
        }
    }

    public String getTableGroupName() {
        if (name instanceof SQLPropertyExpr) {
            return ((SQLPropertyExpr) name).getName();
        } else if (name instanceof SQLIdentifierExpr) {
            return ((SQLIdentifierExpr) name).getName();
        } else if (name instanceof SQLCharExpr) {
            return ((SQLCharExpr) name).getText();
        }
        return null;
    }

    public SQLExpr getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.partitionNum = x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }

    @Override
    public List<SQLObject> getChildren() {
        List<SQLObject> children = new ArrayList<SQLObject>();
        if (name != null) {
            children.add(name);
        }
        return children;
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

}

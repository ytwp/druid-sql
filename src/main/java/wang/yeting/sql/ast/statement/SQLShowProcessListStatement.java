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
import wang.yeting.sql.ast.SQLLimit;
import wang.yeting.sql.ast.SQLOrderBy;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLShowProcessListStatement extends SQLStatementImpl implements SQLShowStatement {

    protected boolean mpp;
    private SQLExpr where;
    private SQLOrderBy orderBy;
    private SQLLimit limit;
    private boolean full = false;

    public boolean isMpp() {
        return mpp;
    }

    public void setMpp(boolean mpp) {
        this.mpp = mpp;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, where);
            acceptChild(visitor, orderBy);
            acceptChild(visitor, limit);
        }
        visitor.endVisit(this);
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

    public SQLOrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(SQLOrderBy x) {
        if (x != null) {
            x.setParent(this);
        }
        this.orderBy = x;
    }

    public SQLLimit getLimit() {
        return limit;
    }

    public void setLimit(SQLLimit x) {
        if (x != null) {
            x.setParent(this);
        }
        this.limit = x;
    }
}

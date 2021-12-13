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

public class SQLPurgeLogsStatement extends SQLStatementImpl implements SQLDropStatement {
    private boolean binary;
    private boolean master;
    private boolean all;

    private SQLExpr to;
    private SQLExpr before;

    public SQLPurgeLogsStatement() {

    }

    public SQLPurgeLogsStatement(DbType dbType) {
        super(dbType);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, to);
            acceptChild(visitor, before);
        }
        visitor.endVisit(this);
    }

    public SQLExpr getTo() {
        return to;
    }

    public void setTo(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.to = x;
    }

    public SQLExpr getBefore() {
        return before;
    }

    public void setBefore(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.before = x;
    }

    public boolean isBinary() {
        return binary;
    }

    public void setBinary(boolean binary) {
        this.binary = binary;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }
}

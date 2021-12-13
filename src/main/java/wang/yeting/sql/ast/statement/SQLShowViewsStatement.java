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
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLReplaceable;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLShowViewsStatement extends SQLStatementImpl implements SQLShowStatement, SQLReplaceable {

    protected SQLName database;
    protected SQLExpr like;


    public SQLName getDatabase() {
        return database;
    }

    public void setDatabase(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }

        this.database = x;
    }

    public SQLExpr getLike() {
        return like;
    }

    public void setLike(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }

        this.like = x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, database);
            acceptChild(visitor, like);
        }
    }

    @Override
    public boolean replace(SQLExpr expr, SQLExpr target) {
        if (database == expr) {
            setDatabase((SQLName) target);
            return true;
        }

        if (like == expr) {
            setLike(target);
            return true;
        }

        return false;
    }
}

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
import wang.yeting.sql.ast.SQLReplaceable;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLDropUserStatement extends SQLStatementImpl implements SQLDropStatement, SQLReplaceable {

    protected boolean ifExists = false;
    private List<SQLExpr> users = new ArrayList<SQLExpr>(2);

    public SQLDropUserStatement() {

    }

    public SQLDropUserStatement(DbType dbType) {
        super(dbType);
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public List<SQLExpr> getUsers() {
        return users;
    }

    public void addUser(SQLExpr user) {
        if (user != null) {
            user.setParent(this);
        }
        this.users.add(user);
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, users);
        }
        visitor.endVisit(this);
    }

    @Override
    public List getChildren() {
        return users;
    }

    public boolean replace(SQLExpr expr, SQLExpr target) {
        for (int i = 0; i < users.size(); i++) {
            if (users.get(i) == expr) {
                target.setParent(this);
                users.set(i, target);
                return true;
            }
        }

        return false;
    }
}

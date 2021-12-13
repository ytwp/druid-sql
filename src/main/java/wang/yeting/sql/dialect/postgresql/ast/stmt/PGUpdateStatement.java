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
package wang.yeting.sql.dialect.postgresql.ast.stmt;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.statement.SQLUpdateStatement;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class PGUpdateStatement extends SQLUpdateStatement implements PGSQLStatement {

    private boolean only = false;

    public PGUpdateStatement() {
        super(DbType.postgresql);
    }

    public boolean isOnly() {
        return only;
    }

    public void setOnly(boolean only) {
        this.only = only;
    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof PGASTVisitor) {
            accept0((PGASTVisitor) visitor);
            return;
        }

        super.accept0(visitor);
    }

    @Override
    public void accept0(PGASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor);
        }
        visitor.endVisit(this);
    }

}
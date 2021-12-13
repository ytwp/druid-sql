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

import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.ast.expr.SQLIdentifierExpr;
import wang.yeting.sql.ast.statement.SQLDropStatement;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class PGDropSchemaStatement extends SQLStatementImpl implements PGSQLStatement, SQLDropStatement {

    private SQLIdentifierExpr schemaName;
    private boolean ifExists;
    private boolean cascade;
    private boolean restrict;

    public SQLIdentifierExpr getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(SQLIdentifierExpr schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public boolean isCascade() {
        return cascade;
    }

    public void setCascade(boolean cascade) {
        this.cascade = cascade;
    }

    public boolean isRestrict() {
        return restrict;
    }

    public void setRestrict(boolean restrict) {
        this.restrict = restrict;
    }

    protected void accept0(SQLASTVisitor visitor) {
        accept0((PGASTVisitor) visitor);
    }

    @Override
    public void accept0(PGASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.schemaName);
        }
        visitor.endVisit(this);
    }
}

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
import wang.yeting.sql.ast.statement.SQLAlterStatement;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class PGAlterSchemaStatement extends SQLStatementImpl implements PGSQLStatement, SQLAlterStatement {

    private SQLIdentifierExpr schemaName;
    private SQLIdentifierExpr newName;
    private SQLIdentifierExpr newOwner;

    public SQLIdentifierExpr getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(SQLIdentifierExpr schemaName) {
        this.schemaName = schemaName;
    }

    protected void accept0(SQLASTVisitor visitor) {
        accept0((PGASTVisitor) visitor);
    }

    public SQLIdentifierExpr getNewName() {
        return newName;
    }

    public void setNewName(SQLIdentifierExpr newName) {
        this.newName = newName;
    }

    public SQLIdentifierExpr getNewOwner() {
        return newOwner;
    }

    public void setNewOwner(SQLIdentifierExpr newOwner) {
        this.newOwner = newOwner;
    }

    @Override
    public void accept0(PGASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.schemaName);
            acceptChild(visitor, this.newName);
            acceptChild(visitor, this.newOwner);
        }
        visitor.endVisit(this);
    }
}

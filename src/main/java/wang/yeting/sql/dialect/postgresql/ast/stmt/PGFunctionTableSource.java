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

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLHint;
import wang.yeting.sql.ast.SQLParameter;
import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.dialect.postgresql.ast.PGSQLObject;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class PGFunctionTableSource extends SQLExprTableSource implements PGSQLObject {

    private final List<SQLParameter> parameters = new ArrayList<SQLParameter>();

    public PGFunctionTableSource() {

    }

    public PGFunctionTableSource(SQLExpr expr) {
        this.expr = expr;
    }

    public List<SQLParameter> getParameters() {
        return parameters;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        this.accept0((PGASTVisitor) visitor);
    }

    public void accept0(PGASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.expr);
            acceptChild(visitor, this.parameters);
        }
        visitor.endVisit(this);
    }

    @Override
    public PGFunctionTableSource clone() {

        PGFunctionTableSource x = new PGFunctionTableSource();

        x.setAlias(this.alias);

        for (SQLParameter e : this.parameters) {
            SQLParameter e2 = e.clone();
            e2.setParent(x);
            x.getParameters().add(e2);
        }

        if (this.flashback != null) {
            x.setFlashback(this.flashback.clone());
        }

        if (this.hints != null) {
            for (SQLHint e : this.hints) {
                SQLHint e2 = e.clone();
                e2.setParent(x);
                x.getHints().add(e2);
            }
        }

        return x;
    }
}

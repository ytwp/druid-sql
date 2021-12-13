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
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLCheck extends SQLConstraintImpl implements SQLTableElement, SQLTableConstraint, SQLReplaceable {

    private SQLExpr expr;
    private Boolean enforced;

    public SQLCheck() {

    }

    public SQLCheck(SQLExpr expr) {
        this.setExpr(expr);
    }

    public SQLExpr getExpr() {
        return expr;
    }

    public void setExpr(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.expr = x;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (getName() != null) {
                getName().accept(visitor);
            }

            if (expr != null) {
                expr.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public void cloneTo(SQLCheck x) {
        super.cloneTo(x);

        if (expr != null) {
            expr = expr.clone();
        }

        x.enforced = enforced;
    }

    public SQLCheck clone() {
        SQLCheck x = new SQLCheck();
        cloneTo(x);
        return x;
    }

    @Override
    public boolean replace(SQLExpr expr, SQLExpr target) {
        if (this.expr == expr) {
            setExpr(target);
            return true;
        }

        if (getName() == expr) {
            setName((SQLName) target);
            return true;
        }

        if (getComment() == expr) {
            setComment(target);
            return true;
        }
        return false;
    }

    public Boolean getEnforced() {
        return enforced;
    }

    public void setEnforced(Boolean enforced) {
        this.enforced = enforced;
    }
}

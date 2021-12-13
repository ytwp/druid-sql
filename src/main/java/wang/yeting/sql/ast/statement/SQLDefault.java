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

public class SQLDefault extends SQLConstraintImpl implements SQLTableElement, SQLTableConstraint, SQLReplaceable {

    private SQLExpr expr;
    private SQLExpr column;
    private boolean withValues = false;

    public SQLDefault() {

    }

    public SQLDefault(SQLExpr expr, SQLExpr column) {
        this.setExpr(expr);
        this.setColumn(column);
    }

    public SQLExpr getColumn() {
        return column;
    }

    public void setColumn(SQLExpr column) {
        this.column = column;
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

    public void cloneTo(SQLDefault x) {
        super.cloneTo(x);

        if (expr != null) {
            x.setExpr(expr.clone());
        }

        if (column != null) {
            x.setColumn(column.clone());
        }

        x.setWithValues(x.isWithValues());
    }

    public SQLDefault clone() {
        SQLDefault x = new SQLDefault();
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

    public boolean isWithValues() {
        return withValues;
    }

    public void setWithValues(boolean withValues) {
        this.withValues = withValues;
    }
}

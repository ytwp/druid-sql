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
package wang.yeting.sql.ast.expr;

import wang.yeting.sql.FastsqlException;
import wang.yeting.sql.ast.SQLExprImpl;
import wang.yeting.sql.ast.statement.SQLTableSource;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class SQLAllColumnExpr extends SQLExprImpl {
    private transient SQLTableSource resolvedTableSource;

    public SQLAllColumnExpr() {

    }

    public void output(Appendable buf) {
        try {
            buf.append('*');
        } catch (IOException e) {
            throw new FastsqlException("output error", e);
        }
    }

    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public int hashCode() {
        return 0;
    }

    public boolean equals(Object o) {
        return o instanceof SQLAllColumnExpr;
    }

    public SQLAllColumnExpr clone() {
        SQLAllColumnExpr x = new SQLAllColumnExpr();

        x.resolvedTableSource = resolvedTableSource;
        return x;
    }

    public SQLTableSource getResolvedTableSource() {
        return resolvedTableSource;
    }

    public void setResolvedTableSource(SQLTableSource resolvedTableSource) {
        this.resolvedTableSource = resolvedTableSource;
    }

    @Override
    public List getChildren() {
        return Collections.emptyList();
    }
}

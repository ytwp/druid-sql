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
package wang.yeting.sql.ast;

import wang.yeting.sql.dialect.oracle.ast.OracleSegmentAttributesImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLPartitionValue extends OracleSegmentAttributesImpl {

    protected final List<SQLExpr> items = new ArrayList<SQLExpr>();
    protected Operator operator;

    public SQLPartitionValue(Operator operator) {
        super();
        this.operator = operator;
    }

    public List<SQLExpr> getItems() {
        return items;
    }

    public void addItem(SQLExpr item) {
        if (item != null) {
            item.setParent(this);
        }
        this.items.add(item);
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, getItems());
        }
        visitor.endVisit(this);
    }

    public SQLPartitionValue clone() {
        SQLPartitionValue x = new SQLPartitionValue(operator);

        for (SQLExpr item : items) {
            SQLExpr item2 = item.clone();
            item2.setParent(x);
            x.items.add(item2);
        }

        return x;
    }

    public static enum Operator {
        LessThan, //
        In, //
        List
    }
}

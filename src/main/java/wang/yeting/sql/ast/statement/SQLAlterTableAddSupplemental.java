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

import wang.yeting.sql.ast.SQLObjectImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLAlterTableAddSupplemental extends SQLObjectImpl implements SQLAlterTableItem {

    private SQLTableElement element;

    public SQLAlterTableAddSupplemental() {

    }


    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, element);
        }
        visitor.endVisit(this);
    }

    public SQLTableElement getElement() {
        return element;
    }

    public void setElement(SQLTableElement x) {
        if (x != null) {
            x.setParent(this);
        }
        this.element = x;
    }

}
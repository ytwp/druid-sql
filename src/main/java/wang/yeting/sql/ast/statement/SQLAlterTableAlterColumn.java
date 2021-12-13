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

import wang.yeting.sql.ast.SQLDataType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLObjectImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLAlterTableAlterColumn extends SQLObjectImpl implements SQLAlterTableItem {
    private SQLName originColumn;
    private SQLColumnDefinition column;
    private boolean setNotNull;
    private boolean dropNotNull;
    private SQLExpr setDefault;
    private boolean dropDefault;
    private boolean first;
    private SQLName after;
    private SQLDataType dataType;
    private boolean toFirst;

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, column);
            acceptChild(visitor, setDefault);
        }
        visitor.endVisit(this);
    }

    public SQLColumnDefinition getColumn() {
        return column;
    }

    public void setColumn(SQLColumnDefinition column) {
        this.column = column;
    }

    public boolean isSetNotNull() {
        return setNotNull;
    }

    public void setSetNotNull(boolean setNotNull) {
        this.setNotNull = setNotNull;
    }

    public boolean isDropNotNull() {
        return dropNotNull;
    }

    public void setDropNotNull(boolean dropNotNull) {
        this.dropNotNull = dropNotNull;
    }

    public SQLExpr getSetDefault() {
        return setDefault;
    }

    public void setSetDefault(SQLExpr setDefault) {
        this.setDefault = setDefault;
    }

    public boolean isDropDefault() {
        return dropDefault;
    }

    public void setDropDefault(boolean dropDefault) {
        this.dropDefault = dropDefault;
    }

    public SQLName getOriginColumn() {
        return originColumn;
    }

    public void setOriginColumn(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.originColumn = x;
    }

    public boolean isFirst() {
        return first;
    }

    public void setFirst(boolean x) {
        this.first = x;
    }

    public SQLName getAfter() {
        return after;
    }

    public void setAfter(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.after = x;
    }

    public SQLDataType getDataType() {
        return dataType;
    }

    public void setDataType(SQLDataType x) {
        if (x != null) {
            x.setParent(this);
        }
        this.dataType = x;
    }
}

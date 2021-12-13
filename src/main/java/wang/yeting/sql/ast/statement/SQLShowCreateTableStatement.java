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
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class SQLShowCreateTableStatement extends SQLStatementImpl implements SQLShowStatement, SQLReplaceable {

    private SQLName name;
    private boolean all;

    private SQLName likeMapping; // for DLA

    public void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
            acceptChild(visitor, likeMapping);
        }
        visitor.endVisit(this);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public SQLName getLikeMapping() {
        return likeMapping;
    }

    public void setLikeMapping(SQLName likeMapping) {
        this.likeMapping = likeMapping;
    }

    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    @Override
    public boolean replace(SQLExpr expr, SQLExpr target) {
        if (name == expr) {
            setName((SQLName) target);
            return true;
        }
        if (likeMapping == expr) {
            setLikeMapping((SQLName) target);
            return true;
        }
        return false;
    }
}

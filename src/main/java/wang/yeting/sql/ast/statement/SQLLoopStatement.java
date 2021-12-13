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

import wang.yeting.sql.ast.SQLCommentHint;
import wang.yeting.sql.ast.SQLStatement;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLLoopStatement extends SQLStatementImpl {

    private final List<SQLStatement> statements = new ArrayList<SQLStatement>();
    private String labelName;

    @Override
    public void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, statements);
        }
        visitor.endVisit(this);
    }

    public List<SQLStatement> getStatements() {
        return statements;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public void addStatement(SQLStatement stmt) {
        if (stmt != null) {
            stmt.setParent(this);
        }
        statements.add(stmt);
    }

    @Override
    public List getChildren() {
        return statements;
    }

    @Override
    public SQLLoopStatement clone() {
        SQLLoopStatement x = new SQLLoopStatement();

        x.setLabelName(this.labelName);

        x.setAfterSemi(this.afterSemi);

        x.setDbType(this.dbType);

        for (SQLStatement item : statements) {
            SQLStatement item2 = item.clone();
            item2.setParent(x);
            x.statements.add(item2);
        }

        if (this.headHints != null) {
            List<SQLCommentHint> headHintsClone = new ArrayList<SQLCommentHint>(this.headHints.size());
            for (SQLCommentHint hint : headHints) {
                SQLCommentHint h2 = hint.clone();
                h2.setParent(x);
                headHintsClone.add(h2);
            }
            x.setHeadHints(headHintsClone);
        }

        return x;
    }
}

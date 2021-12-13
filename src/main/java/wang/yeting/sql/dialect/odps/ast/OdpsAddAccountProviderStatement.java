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
package wang.yeting.sql.dialect.odps.ast;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.dialect.odps.visitor.OdpsASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitor;

public class OdpsAddAccountProviderStatement extends SQLStatementImpl {

    private SQLName provider;

    public OdpsAddAccountProviderStatement() {
        super(DbType.odps);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        accept0((OdpsASTVisitor) visitor);
    }

    public void accept0(OdpsASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, provider);
        }
        visitor.endVisit(this);
    }

    public SQLName getProvider() {
        return provider;
    }

    public void setProvider(SQLName provider) {
        if (provider != null) {
            provider.setParent(this);
        }
        this.provider = provider;
    }
}

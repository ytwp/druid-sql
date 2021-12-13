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
package wang.yeting.sql.dialect.sqlserver.visitor;

import wang.yeting.sql.DbType;
import wang.yeting.sql.dialect.sqlserver.ast.SQLServerOutput;
import wang.yeting.sql.dialect.sqlserver.ast.SQLServerTop;
import wang.yeting.sql.dialect.sqlserver.ast.expr.SQLServerObjectReferenceExpr;
import wang.yeting.sql.dialect.sqlserver.ast.stmt.*;
import wang.yeting.sql.dialect.sqlserver.ast.stmt.SQLServerExecStatement.SQLServerParameter;
import wang.yeting.sql.repository.SchemaRepository;
import wang.yeting.sql.stat.TableStat;
import wang.yeting.sql.visitor.SchemaStatVisitor;

public class SQLServerSchemaStatVisitor extends SchemaStatVisitor implements SQLServerASTVisitor {
    public SQLServerSchemaStatVisitor() {
        super(DbType.sqlserver);
    }

    public SQLServerSchemaStatVisitor(SchemaRepository repository) {
        super(repository);
    }

    @Override
    public boolean visit(SQLServerTop x) {
        return false;
    }

    @Override
    public boolean visit(SQLServerObjectReferenceExpr x) {
        return false;
    }

    @Override
    public boolean visit(SQLServerUpdateStatement x) {
        TableStat stat = getTableStat(x.getTableName());
        stat.incrementUpdateCount();

        accept(x.getItems());
        accept(x.getFrom());
        accept(x.getWhere());

        return false;
    }

    @Override
    public boolean visit(SQLServerExecStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLServerSetTransactionIsolationLevelStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLServerOutput x) {
        return false;
    }

    @Override
    public boolean visit(SQLServerRollbackStatement x) {
        return true;
    }

    @Override
    public boolean visit(SQLServerWaitForStatement x) {
        return true;
    }

    @Override
    public boolean visit(SQLServerParameter x) {
        return false;
    }
}

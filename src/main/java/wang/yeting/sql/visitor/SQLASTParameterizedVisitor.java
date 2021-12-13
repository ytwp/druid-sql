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
package wang.yeting.sql.visitor;

import wang.yeting.sql.DbType;
import wang.yeting.sql.SQLUtils;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLObject;
import wang.yeting.sql.ast.statement.SQLInsertStatement;
import wang.yeting.sql.ast.statement.SQLInsertStatement.ValuesClause;
import wang.yeting.sql.ast.statement.SQLSelectGroupByClause;
import wang.yeting.sql.ast.statement.SQLSelectOrderByItem;
import wang.yeting.sql.util.FnvHash;

import java.util.List;

public class SQLASTParameterizedVisitor extends SQLASTVisitorAdapter {

    protected DbType dbType;

    protected List<Object> parameters;
    private int replaceCount = 0;

    public SQLASTParameterizedVisitor(DbType dbType) {
        this.dbType = dbType;
    }

    public SQLASTParameterizedVisitor(DbType dbType, List<Object> parameters) {
        this.dbType = dbType;
        this.parameters = parameters;
    }

    public int getReplaceCount() {
        return this.replaceCount;
    }

    public void incrementReplaceCunt() {
        replaceCount++;
    }

    public DbType getDbType() {
        return dbType;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    public void setParameters(List<Object> parameters) {
        this.parameters = parameters;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLTimestampExpr x) {
        parameterizeAndExportPara(x);
        return false;
    }

    public void parameterizeAndExportPara(SQLExpr x) {
        wang.yeting.sql.ast.expr.SQLVariantRefExpr variantRefExpr = new wang.yeting.sql.ast.expr.SQLVariantRefExpr("?");
        variantRefExpr.setIndex(this.replaceCount);

        SQLUtils.replaceInParent(x, variantRefExpr);
        incrementReplaceCunt();
        ExportParameterVisitorUtils.exportParameter(this.parameters, x);
    }

    public void parameterize(SQLExpr x) {
        wang.yeting.sql.ast.expr.SQLVariantRefExpr variantRefExpr = new wang.yeting.sql.ast.expr.SQLVariantRefExpr("?");
        variantRefExpr.setIndex(this.replaceCount);

        SQLUtils.replaceInParent(x, variantRefExpr);
        incrementReplaceCunt();
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLCharExpr x) {
        parameterizeAndExportPara(x);
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLSelectGroupByClause || parent instanceof SQLSelectOrderByItem) {
            return false;
        }
        parameterizeAndExportPara(x);
        return false;
    }


    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {
        List<SQLExpr> arguments = x.getArguments();
        if (x.methodNameHashCode64() == FnvHash.Constants.TRIM
                && arguments.size() == 1
                && arguments.get(0) instanceof wang.yeting.sql.ast.expr.SQLCharExpr && x.getTrimOption() == null && x.getFrom() == null) {
            parameterizeAndExportPara(x);

            if (this.parameters != null) {
                wang.yeting.sql.ast.expr.SQLCharExpr charExpr = (wang.yeting.sql.ast.expr.SQLCharExpr) arguments.get(0);
                this.parameters.add(charExpr.getText().trim());
            }

            replaceCount++;
            return false;
        }
        return true;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLNCharExpr x) {
        parameterizeAndExportPara(x);
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLNullExpr x) {
        SQLObject parent = x.getParent();
        if (parent instanceof SQLInsertStatement || parent instanceof ValuesClause || parent instanceof wang.yeting.sql.ast.expr.SQLInListExpr ||
                (parent instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr && ((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) parent).getOperator() == wang.yeting.sql.ast.expr.SQLBinaryOperator.Equality)) {
            parameterize(x);

            if (this.parameters != null) {
                if (parent instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                    ExportParameterVisitorUtils.exportParameter(parameters, x);
                } else {
                    this.parameters.add(null);
                }
            }
            return false;
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLNumberExpr x) {
        parameterizeAndExportPara(x);
        return false;
    }


    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLHexExpr x) {
        parameterizeAndExportPara(x);
        return false;
    }
}

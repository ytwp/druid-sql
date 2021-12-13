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

import wang.yeting.sql.visitor.functions.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLEvalVisitorImpl extends SQLASTVisitorAdapter implements SQLEvalVisitor {

    private List<Object> parameters = new ArrayList<Object>();

    private Map<String, Function> functions = new HashMap<String, Function>();

    private int variantIndex = -1;

    private boolean markVariantIndex = true;

    public SQLEvalVisitorImpl() {
        this(new ArrayList<Object>(1));
    }

    public SQLEvalVisitorImpl(List<Object> parameters) {
        this.parameters = parameters;
    }

    @Override
    public List<Object> getParameters() {
        return parameters;
    }

    @Override
    public void setParameters(List<Object> parameters) {
        this.parameters = parameters;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLCharExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public int incrementAndGetVariantIndex() {
        return ++variantIndex;
    }

    public int getVariantIndex() {
        return variantIndex;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLVariantRefExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLNumberExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLHexExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLCaseExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLInListExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLNullExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLQueryExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public boolean isMarkVariantIndex() {
        return markVariantIndex;
    }

    @Override
    public void setMarkVariantIndex(boolean markVariantIndex) {
        this.markVariantIndex = markVariantIndex;
    }

    @Override
    public Function getFunction(String funcName) {
        return functions.get(funcName);
    }

    @Override
    public void registerFunction(String funcName, Function function) {
        functions.put(funcName, function);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }

    @Override
    public void unregisterFunction(String funcName) {
        functions.remove(funcName);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLBooleanExpr x) {
        x.getAttributes().put(EVAL_VALUE, x.getBooleanValue());
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLBinaryExpr x) {
        return SQLEvalVisitorUtils.visit(this, x);
    }
}

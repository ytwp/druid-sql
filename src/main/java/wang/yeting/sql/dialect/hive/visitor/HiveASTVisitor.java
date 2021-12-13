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
package wang.yeting.sql.dialect.hive.visitor;

import wang.yeting.sql.dialect.hive.ast.HiveInsert;
import wang.yeting.sql.dialect.hive.ast.HiveInsertStatement;
import wang.yeting.sql.dialect.hive.ast.HiveMultiInsertStatement;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateFunctionStatement;
import wang.yeting.sql.dialect.hive.stmt.HiveLoadDataStatement;
import wang.yeting.sql.dialect.hive.stmt.HiveMsckRepairStatement;
import wang.yeting.sql.visitor.SQLASTVisitor;

public interface HiveASTVisitor extends SQLASTVisitor {
    default boolean visit(HiveInsert x) {
        return true;
    }

    default void endVisit(HiveInsert x) {
    }

    default boolean visit(HiveMultiInsertStatement x) {
        return true;
    }

    default void endVisit(HiveMultiInsertStatement x) {
    }

    default boolean visit(HiveInsertStatement x) {
        return true;
    }

    default void endVisit(HiveInsertStatement x) {
    }

    default boolean visit(HiveCreateFunctionStatement x) {
        return true;
    }

    default void endVisit(HiveCreateFunctionStatement x) {
    }

    default boolean visit(HiveLoadDataStatement x) {
        return true;
    }

    default void endVisit(HiveLoadDataStatement x) {
    }

    default boolean visit(HiveMsckRepairStatement x) {
        return true;
    }

    default void endVisit(HiveMsckRepairStatement x) {
    }

}

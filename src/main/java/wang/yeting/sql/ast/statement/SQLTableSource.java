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
import wang.yeting.sql.ast.SQLHint;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLObject;

import java.util.List;

public interface SQLTableSource extends SQLObject {

    String getAlias();

    void setAlias(String alias);

    long aliasHashCode64();

    List<SQLHint> getHints();

    SQLTableSource clone();

    String computeAlias();

    boolean containsAlias(String alias);

    SQLExpr getFlashback();

    void setFlashback(SQLExpr flashback);

    SQLColumnDefinition findColumn(String columnName);

    SQLColumnDefinition findColumn(long columnNameHash);

    SQLObject resolveColum(long columnNameHash);

    SQLTableSource findTableSourceWithColumn(String columnName);

    SQLTableSource findTableSourceWithColumn(long columnName_hash);

    SQLTableSource findTableSourceWithColumn(SQLName columnName);

    SQLTableSource findTableSourceWithColumn(long columnName_hash, String name, int option);

    SQLTableSource findTableSource(String alias);

    SQLTableSource findTableSource(long alias_hash);
}

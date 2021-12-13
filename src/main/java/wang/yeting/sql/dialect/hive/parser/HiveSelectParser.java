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
package wang.yeting.sql.dialect.hive.parser;

import wang.yeting.sql.ast.statement.SQLTableSource;
import wang.yeting.sql.parser.SQLExprParser;
import wang.yeting.sql.parser.SQLSelectListCache;
import wang.yeting.sql.parser.SQLSelectParser;

public class HiveSelectParser extends SQLSelectParser {

    public HiveSelectParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public HiveSelectParser(SQLExprParser exprParser, SQLSelectListCache selectListCache) {
        super(exprParser, selectListCache);
    }

    public HiveSelectParser(String sql) {
        this(new HiveExprParser(sql));
    }

    protected SQLExprParser createExprParser() {
        return new HiveExprParser(lexer);
    }

    public void parseTableSourceSample(SQLTableSource tableSource) {
        parseTableSourceSampleHive(tableSource);
    }
}

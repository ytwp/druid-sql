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
package wang.yeting.sql.ast;

import wang.yeting.sql.DbType;

import java.util.List;

public interface SQLDataType extends SQLObject {

    String getName();

    void setName(String name);

    long nameHashCode64();

    List<SQLExpr> getArguments();

    Boolean getWithTimeZone();

    void setWithTimeZone(Boolean value);

    boolean isWithLocalTimeZone();

    void setWithLocalTimeZone(boolean value);

    SQLDataType clone();

    DbType getDbType();

    void setDbType(DbType dbType);

    int jdbcType();

    boolean isInt();

    boolean isNumberic();

    public boolean isString();

    public boolean hasKeyLength();

    interface Constants {
        String CHAR = "CHAR";
        String NCHAR = "NCHAR";
        String VARCHAR = "VARCHAR";
        String VARBINARY = "VARBINARY";
        String DATE = "DATE";
        String TIMESTAMP = "TIMESTAMP";
        String XML = "XML";

        String DECIMAL = "DECIMAL";
        String NUMBER = "NUMBER";
        String REAL = "REAL";
        String DOUBLE_PRECISION = "DOUBLE PRECISION";
        String DOUBLE = "DOUBLE";

        String TINYINT = "TINYINT";
        String SMALLINT = "SMALLINT";
        String INT = "INT";
        String BIGINT = "BIGINT";
        String TEXT = "TEXT";
        String BYTEA = "BYTEA";
        String BOOLEAN = "BOOLEAN";

        String FLOAT = "FLOAT";
    }
}

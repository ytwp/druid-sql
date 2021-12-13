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
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLObject;
import wang.yeting.sql.dialect.db2.visitor.DB2ExportParameterVisitor;
import wang.yeting.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import wang.yeting.sql.dialect.oracle.visitor.OracleExportParameterVisitor;
import wang.yeting.sql.dialect.postgresql.visitor.PGExportParameterVisitor;
import wang.yeting.sql.dialect.sqlserver.visitor.MSSQLServerExportParameterVisitor;

import java.util.ArrayList;
import java.util.List;

public final class ExportParameterVisitorUtils {

    //private for util class not need new instance
    private ExportParameterVisitorUtils() {
        super();
    }

    public static ExportParameterVisitor createExportParameterVisitor(Appendable out, DbType dbType) {
        if (dbType == null) {
            dbType = DbType.other;
        }

        switch (dbType) {
            case mysql:
            case mariadb:
                return new MySqlExportParameterVisitor(out);
            case oracle:
                return new OracleExportParameterVisitor(out);
            case db2:
                return new DB2ExportParameterVisitor(out);
            case h2:
                return new MySqlExportParameterVisitor(out);
            case sqlserver:
            case jtds:
                return new MSSQLServerExportParameterVisitor(out);
            case postgresql:
            case edb:
                return new PGExportParameterVisitor(out);
            default:
                return new ExportParameterizedOutputVisitor(out);
        }
    }


    public static boolean exportParamterAndAccept(final List<Object> parameters, List<SQLExpr> list) {
        for (int i = 0, size = list.size(); i < size; ++i) {
            SQLExpr param = list.get(i);

            SQLExpr result = exportParameter(parameters, param);
            if (result != param) {
                list.set(i, result);
            }
        }

        return false;
    }

    public static SQLExpr exportParameter(final List<Object> parameters, final SQLExpr param) {
        Object value = null;
        boolean replace = false;

        if (param instanceof wang.yeting.sql.ast.expr.SQLCharExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLCharExpr) param).getText();
            String vStr = (String) value;
//            if (vStr.length() > 1) {
//                value = StringUtils.removeNameQuotes(vStr);
//            }
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLNCharExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLNCharExpr) param).getText();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLBooleanExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLBooleanExpr) param).getBooleanValue();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLNumericLiteralExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLNumericLiteralExpr) param).getNumber();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLHexExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLHexExpr) param).toBytes();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLTimestampExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLTimestampExpr) param).getValue();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLDateExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLDateExpr) param).getValue();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLTimeExpr) {
            value = ((wang.yeting.sql.ast.expr.SQLTimeExpr) param).getValue();
            replace = true;
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLListExpr) {
            wang.yeting.sql.ast.expr.SQLListExpr list = ((wang.yeting.sql.ast.expr.SQLListExpr) param);

            List<Object> listValues = new ArrayList<Object>();
            for (int i = 0; i < list.getItems().size(); i++) {
                SQLExpr listItem = list.getItems().get(i);

                if (listItem instanceof wang.yeting.sql.ast.expr.SQLCharExpr) {
                    Object listValue = ((wang.yeting.sql.ast.expr.SQLCharExpr) listItem).getText();
                    listValues.add(listValue);
                } else if (listItem instanceof wang.yeting.sql.ast.expr.SQLBooleanExpr) {
                    Object listValue = ((wang.yeting.sql.ast.expr.SQLBooleanExpr) listItem).getBooleanValue();
                    listValues.add(listValue);
                } else if (listItem instanceof wang.yeting.sql.ast.expr.SQLNumericLiteralExpr) {
                    Object listValue = ((wang.yeting.sql.ast.expr.SQLNumericLiteralExpr) listItem).getNumber();
                    listValues.add(listValue);
                } else if (param instanceof wang.yeting.sql.ast.expr.SQLHexExpr) {
                    Object listValue = ((wang.yeting.sql.ast.expr.SQLHexExpr) listItem).toBytes();
                    listValues.add(listValue);
                }
            }

            if (listValues.size() == list.getItems().size()) {
                value = listValues;
                replace = true;
            }
        } else if (param instanceof wang.yeting.sql.ast.expr.SQLNullExpr) {
            value = null;
            replace = true;
        }

        if (replace) {
            SQLObject parent = param.getParent();
            if (parent != null) {
                List<SQLObject> mergedList = null;
                if (parent instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                    mergedList = ((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) parent).getMergedList();
                }
                if (mergedList != null) {
                    List<Object> mergedListParams = new ArrayList<Object>(mergedList.size() + 1);
                    for (int i = 0; i < mergedList.size(); ++i) {
                        SQLObject item = mergedList.get(i);
                        if (item instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                            wang.yeting.sql.ast.expr.SQLBinaryOpExpr binaryOpItem = (wang.yeting.sql.ast.expr.SQLBinaryOpExpr) item;
                            exportParameter(mergedListParams, binaryOpItem.getRight());
                        }
                    }
                    if (mergedListParams.size() > 0) {
                        mergedListParams.add(0, value);
                        value = mergedListParams;
                    }
                }
            }

            if (parameters != null) {
                parameters.add(value);
            }

            return new wang.yeting.sql.ast.expr.SQLVariantRefExpr("?");
        }

        return param;
    }

    public static void exportParameter(final List<Object> parameters, wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
        if (x.getLeft() instanceof wang.yeting.sql.ast.expr.SQLLiteralExpr
                && x.getRight() instanceof wang.yeting.sql.ast.expr.SQLLiteralExpr
                && x.getOperator().isRelational()) {
            return;
        }

        {
            SQLExpr leftResult = ExportParameterVisitorUtils.exportParameter(parameters, x.getLeft());
            if (leftResult != x.getLeft()) {
                x.setLeft(leftResult);
            }
        }

        {
            SQLExpr rightResult = exportParameter(parameters, x.getRight());
            if (rightResult != x.getRight()) {
                x.setRight(rightResult);
            }
        }
    }

    public static void exportParameter(final List<Object> parameters, wang.yeting.sql.ast.expr.SQLBetweenExpr x) {
        {
            SQLExpr result = exportParameter(parameters, x.getBeginExpr());
            if (result != x.getBeginExpr()) {
                x.setBeginExpr(result);
            }
        }

        {
            SQLExpr result = exportParameter(parameters, x.getEndExpr());
            if (result != x.getBeginExpr()) {
                x.setEndExpr(result);
            }
        }

    }
}

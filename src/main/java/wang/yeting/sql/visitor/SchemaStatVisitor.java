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
import wang.yeting.sql.dialect.hive.ast.HiveInsert;
import wang.yeting.sql.dialect.hive.ast.HiveMultiInsertStatement;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateTableStatement;
import wang.yeting.sql.dialect.mysql.ast.expr.MySqlExpr;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import wang.yeting.sql.dialect.oracle.ast.expr.OracleExpr;
import wang.yeting.sql.dialect.oracle.visitor.OracleASTVisitorAdapter;
import wang.yeting.sql.dialect.postgresql.visitor.PGASTVisitorAdapter;
import wang.yeting.sql.repository.SchemaObject;
import wang.yeting.sql.repository.SchemaRepository;
import wang.yeting.sql.stat.TableStat;
import wang.yeting.sql.stat.TableStat.Column;
import wang.yeting.sql.stat.TableStat.Condition;
import wang.yeting.sql.stat.TableStat.Mode;
import wang.yeting.sql.stat.TableStat.Relationship;
import wang.yeting.sql.util.FnvHash;

import java.util.*;

public class SchemaStatVisitor extends SQLASTVisitorAdapter {

    protected final List<wang.yeting.sql.ast.SQLName> originalTables = new ArrayList<wang.yeting.sql.ast.SQLName>();
    protected final HashMap<TableStat.Name, TableStat> tableStats = new LinkedHashMap<TableStat.Name, TableStat>();
    protected final Map<Long, Column> columns = new LinkedHashMap<Long, Column>();
    protected final List<Condition> conditions = new ArrayList<Condition>();
    protected final Set<Relationship> relationships = new LinkedHashSet<Relationship>();
    protected final List<Column> orderByColumns = new ArrayList<Column>();
    protected final Set<Column> groupByColumns = new LinkedHashSet<Column>();
    protected final List<wang.yeting.sql.ast.expr.SQLAggregateExpr> aggregateFunctions = new ArrayList<wang.yeting.sql.ast.expr.SQLAggregateExpr>();
    protected final List<wang.yeting.sql.ast.expr.SQLMethodInvokeExpr> functions = new ArrayList<wang.yeting.sql.ast.expr.SQLMethodInvokeExpr>(2);
    protected SchemaRepository repository;
    protected DbType dbType;
    private List<Object> parameters;
    private Mode mode;

    public SchemaStatVisitor() {
        this((DbType) null);
    }

    public SchemaStatVisitor(SchemaRepository repository) {
        if (repository != null) {
            this.dbType = repository.getDbType();
        }
        this.repository = repository;
    }

    public SchemaStatVisitor(DbType dbType) {
        this(new SchemaRepository(dbType), new ArrayList<Object>());
        this.dbType = dbType;
    }

    public SchemaStatVisitor(List<Object> parameters) {
        this((DbType) null, parameters);
    }

    public SchemaStatVisitor(DbType dbType, List<Object> parameters) {
        this(new SchemaRepository(dbType), parameters);
        this.parameters = parameters;
    }

    public SchemaStatVisitor(SchemaRepository repository, List<Object> parameters) {
        this.repository = repository;
        this.parameters = parameters;
        if (repository != null) {
            DbType dbType = repository.getDbType();
            if (dbType != null && this.dbType == null) {
                this.dbType = dbType;
            }
        }
    }

    protected static void putAliasMap(Map<String, String> aliasMap, String name, String value) {
        if (aliasMap == null || name == null) {
            return;
        }
        aliasMap.put(name.toLowerCase(), value);
    }

    private static boolean isParam(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        if (x.getResolvedParameter() != null
                || x.getResolvedDeclareItem() != null) {
            return true;
        }
        return false;
    }

    public SchemaRepository getRepository() {
        return repository;
    }

    public void setRepository(SchemaRepository repository) {
        this.repository = repository;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    public void setParameters(List<Object> parameters) {
        this.parameters = parameters;
    }

    public TableStat getTableStat(String tableName) {
        tableName = handleName(tableName);

        TableStat.Name tableNameObj = new TableStat.Name(tableName);
        TableStat stat = tableStats.get(tableNameObj);
        if (stat == null) {
            stat = new TableStat();
            tableStats.put(new TableStat.Name(tableName), stat);
        }
        return stat;
    }

    public TableStat getTableStat(wang.yeting.sql.ast.SQLName tableName) {
        String strName;
        if (tableName instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            strName = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) tableName).normalizedName();
        } else if (tableName instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
            strName = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) tableName).normalizedName();
        } else {
            strName = tableName.toString();
        }

        long hashCode64 = tableName.hashCode64();

        if (hashCode64 == FnvHash.Constants.DUAL) {
            return null;
        }

        originalTables.add(tableName);

        TableStat.Name tableNameObj = new TableStat.Name(strName, hashCode64);
        TableStat stat = tableStats.get(tableNameObj);
        if (stat == null) {
            stat = new TableStat();
            tableStats.put(new TableStat.Name(strName, hashCode64), stat);
        }
        return stat;
    }

    protected Column addColumn(String tableName, String columnName) {
        Column c = new Column(tableName, columnName, dbType);

        Column column = this.columns.get(c.hashCode64());
        if (column == null && columnName != null) {
            column = c;
            columns.put(c.hashCode64(), c);
        }
        return column;
    }

    protected Column addColumn(wang.yeting.sql.ast.SQLName table, String columnName) {
        String tableName;
        if (table instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            tableName = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) table).normalizedName();
        } else if (table instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
            tableName = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) table).normalizedName();
        } else {
            tableName = table.toString();
        }

        long tableHashCode64 = table.hashCode64();

        long basic = tableHashCode64;
        basic ^= '.';
        basic *= FnvHash.PRIME;
        long columnHashCode64 = FnvHash.hashCode64(basic, columnName);

        Column column = this.columns.get(columnHashCode64);
        if (column == null && columnName != null) {
            column = new Column(tableName, columnName, columnHashCode64);
            columns.put(columnHashCode64, column);
        }
        return column;
    }

    private String handleName(String ident) {
        int len = ident.length();
        if (ident.charAt(0) == '[' && ident.charAt(len - 1) == ']') {
            ident = ident.substring(1, len - 1);
        } else {
            boolean flag0 = false;
            boolean flag1 = false;
            boolean flag2 = false;
            boolean flag3 = false;
            for (int i = 0; i < len; ++i) {
                final char ch = ident.charAt(i);
                if (ch == '\"') {
                    flag0 = true;
                } else if (ch == '`') {
                    flag1 = true;
                } else if (ch == ' ') {
                    flag2 = true;
                } else if (ch == '\'') {
                    flag3 = true;
                }
            }
            if (flag0) {
                ident = ident.replaceAll("\"", "");
            }

            if (flag1) {
                ident = ident.replaceAll("`", "");
            }

            if (flag2) {
                ident = ident.replaceAll(" ", "");
            }

            if (flag3) {
                ident = ident.replaceAll("'", "");
            }
        }
        return ident;
    }

    protected Mode getMode() {
        return mode;
    }

    protected void setModeOrigin(wang.yeting.sql.ast.SQLObject x) {
        Mode originalMode = (Mode) x.getAttribute("_original_use_mode");
        mode = originalMode;
    }

    protected Mode setMode(wang.yeting.sql.ast.SQLObject x, Mode mode) {
        Mode oldMode = this.mode;
        x.putAttribute("_original_use_mode", oldMode);
        this.mode = mode;
        return oldMode;
    }

    private boolean visitOrderBy(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();

        if (tableSource == null
                && x.getParent() instanceof wang.yeting.sql.ast.statement.SQLSelectOrderByItem
                && x.getParent().getParent() instanceof wang.yeting.sql.ast.SQLOrderBy
                && x.getParent().getParent().getParent() instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) x.getParent().getParent().getParent();
            wang.yeting.sql.ast.statement.SQLSelectItem selectItem = queryBlock.findSelectItem(x.nameHashCode64());
            if (selectItem != null) {
                wang.yeting.sql.ast.SQLExpr selectItemExpr = selectItem.getExpr();
                if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    x = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr;
                } else if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    return visitOrderBy((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr);
                } else {
                    return false;
                }
            }
        }

        String tableName = null;
        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr table = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
                tableName = table.getName();
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                wang.yeting.sql.ast.expr.SQLPropertyExpr table = (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr;
                tableName = table.toString();
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr methodInvokeExpr = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if ("table".equalsIgnoreCase(methodInvokeExpr.getMethodName())
                        && methodInvokeExpr.getArguments().size() == 1
                        && methodInvokeExpr.getArguments().get(0) instanceof wang.yeting.sql.ast.SQLName) {
                    wang.yeting.sql.ast.SQLName table = (wang.yeting.sql.ast.SQLName) methodInvokeExpr.getArguments().get(0);

                    if (table instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                        wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = (wang.yeting.sql.ast.expr.SQLPropertyExpr) table;
                        wang.yeting.sql.ast.expr.SQLIdentifierExpr owner = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) propertyExpr.getOwner();
                        if (propertyExpr.getResolvedTableSource() != null
                                && propertyExpr.getResolvedTableSource() instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                            wang.yeting.sql.ast.SQLExpr resolveExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) propertyExpr.getResolvedTableSource()).getExpr();
                            if (resolveExpr instanceof wang.yeting.sql.ast.SQLName) {
                                tableName = resolveExpr.toString() + "." + propertyExpr.getName();
                            }
                        }
                    }

                    if (tableName == null) {
                        tableName = table.toString();
                    }
                }
            }
        } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) {
            return false;
        } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) tableSource).getSelect().getQueryBlock();
            if (queryBlock == null) {
                return false;
            }

            wang.yeting.sql.ast.statement.SQLSelectItem selectItem = queryBlock.findSelectItem(x.nameHashCode64());
            if (selectItem == null) {
                return false;
            }

            wang.yeting.sql.ast.SQLExpr selectItemExpr = selectItem.getExpr();
            wang.yeting.sql.ast.statement.SQLTableSource columnTableSource = null;
            if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                columnTableSource = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr).getResolvedTableSource();
            } else if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                columnTableSource = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr).getResolvedTableSource();
            }

            if (columnTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource && ((wang.yeting.sql.ast.statement.SQLExprTableSource) columnTableSource).getExpr() instanceof wang.yeting.sql.ast.SQLName) {
                wang.yeting.sql.ast.SQLName tableExpr = (wang.yeting.sql.ast.SQLName) ((wang.yeting.sql.ast.statement.SQLExprTableSource) columnTableSource).getExpr();
                if (tableExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    tableName = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) tableExpr).normalizedName();
                } else if (tableExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    tableName = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) tableExpr).normalizedName();
                }
            }
        } else {
            boolean skip = false;
            for (wang.yeting.sql.ast.SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
                if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                    wang.yeting.sql.ast.statement.SQLTableSource from = ((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) parent).getFrom();

                    if (from instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource) {
                        skip = true;
                        break;
                    }
                } else if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQuery) {
                    break;
                }
            }
        }

        String identName = x.getName();
        if (tableName != null) {
            orderByAddColumn(tableName, identName, x);
        } else {
            orderByAddColumn("UNKNOWN", identName, x);
        }
        return false;
    }

    private boolean visitOrderBy(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
        if (isSubQueryOrParamOrVariant(x)) {
            return false;
        }

        String owner = null;

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();
        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr tableSourceExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();
            if (tableSourceExpr instanceof wang.yeting.sql.ast.SQLName) {
                owner = tableSourceExpr.toString();
            }
        }

        if (owner == null && x.getOwner() instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            owner = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) x.getOwner()).getName();
        }

        if (owner == null) {
            return false;
        }

        orderByAddColumn(owner, x.getName(), x);

        return false;
    }

    private boolean visitOrderBy(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
        wang.yeting.sql.ast.SQLObject parent = x.getParent();
        if (!(parent instanceof wang.yeting.sql.ast.statement.SQLSelectOrderByItem)) {
            return false;
        }

        if (parent.getParent() instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
            int selectItemIndex = x.getNumber().intValue() - 1;
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) parent.getParent();
            final List<wang.yeting.sql.ast.statement.SQLSelectItem> selectList = queryBlock.getSelectList();

            if (selectItemIndex < 0 || selectItemIndex >= selectList.size()) {
                return false;
            }

            wang.yeting.sql.ast.SQLExpr selectItemExpr = selectList.get(selectItemIndex).getExpr();
            if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                visitOrderBy((wang.yeting.sql.ast.expr.SQLIdentifierExpr) selectItemExpr);
            } else if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                visitOrderBy((wang.yeting.sql.ast.expr.SQLPropertyExpr) selectItemExpr);
            }
        }

        return false;
    }

    private void orderByAddColumn(String table, String columnName, wang.yeting.sql.ast.SQLObject expr) {
        Column column = new Column(table, columnName, dbType);

        wang.yeting.sql.ast.SQLObject parent = expr.getParent();
        if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectOrderByItem) {
            wang.yeting.sql.ast.SQLOrderingSpecification type = ((wang.yeting.sql.ast.statement.SQLSelectOrderByItem) parent).getType();
            column.getAttributes().put("orderBy.type", type);
        }

        orderByColumns.add(column);
    }

    public boolean visit(wang.yeting.sql.ast.SQLOrderBy x) {
        final SQLASTVisitor orderByVisitor = createOrderByVisitor(x);

        wang.yeting.sql.ast.statement.SQLSelectQueryBlock query = null;
        if (x.getParent() instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
            query = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) x.getParent();
        }
        if (query != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : x.getItems()) {
                wang.yeting.sql.ast.SQLExpr expr = item.getExpr();
                if (expr instanceof wang.yeting.sql.ast.expr.SQLIntegerExpr) {
                    int intValue = ((wang.yeting.sql.ast.expr.SQLIntegerExpr) expr).getNumber().intValue() - 1;
                    if (intValue < query.getSelectList().size()) {
                        wang.yeting.sql.ast.SQLExpr selectItemExpr = query.getSelectList().get(intValue).getExpr();
                        selectItemExpr.accept(orderByVisitor);
                    }
                } else if (expr instanceof MySqlExpr || expr instanceof OracleExpr) {
                    continue;
                }
            }
        }
        x.accept(orderByVisitor);

        for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem orderByItem : x.getItems()) {
            statExpr(
                    orderByItem.getExpr());
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.SQLOver x) {
        wang.yeting.sql.ast.SQLName of = x.getOf();
        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        List<wang.yeting.sql.ast.SQLExpr> partitionBy = x.getPartitionBy();


        if (of == null // skip if of is not null
                && orderBy != null) {
            orderBy.accept(this);
        }

        if (partitionBy != null) {
            for (wang.yeting.sql.ast.SQLExpr expr : partitionBy) {
                expr.accept(this);
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.SQLWindow x) {
        wang.yeting.sql.ast.SQLOver over = x.getOver();
        if (over != null) {
            visit(over);
        }
        return false;
    }

    protected SQLASTVisitor createOrderByVisitor(wang.yeting.sql.ast.SQLOrderBy x) {
        if (dbType == null) {
            dbType = DbType.other;
        }

        final SQLASTVisitor orderByVisitor;
        switch (dbType) {
            case mysql:
                return new MySqlOrderByStatVisitor(x);
            case postgresql:
                return new PGOrderByStatVisitor(x);
            case oracle:
                return new OracleOrderByStatVisitor(x);
            default:
                return new OrderByStatVisitor(x);
        }
    }

    public Set<Relationship> getRelationships() {
        return relationships;
    }

    public List<Column> getOrderByColumns() {
        return orderByColumns;
    }

    public Set<Column> getGroupByColumns() {
        return groupByColumns;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public List<wang.yeting.sql.ast.expr.SQLAggregateExpr> getAggregateFunctions() {
        return aggregateFunctions;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLBetweenExpr x) {
        wang.yeting.sql.ast.SQLObject parent = x.getParent();

        wang.yeting.sql.ast.SQLExpr test = x.getTestExpr();
        wang.yeting.sql.ast.SQLExpr begin = x.getBeginExpr();
        wang.yeting.sql.ast.SQLExpr end = x.getEndExpr();

        statExpr(test);
        statExpr(begin);
        statExpr(end);

        handleCondition(test, "BETWEEN", begin, end);

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLBinaryOpExpr x) {
        wang.yeting.sql.ast.SQLObject parent = x.getParent();

        if (parent instanceof wang.yeting.sql.ast.statement.SQLIfStatement) {
            return true;
        }

        final wang.yeting.sql.ast.expr.SQLBinaryOperator op = x.getOperator();
        final wang.yeting.sql.ast.SQLExpr left = x.getLeft();
        final wang.yeting.sql.ast.SQLExpr right = x.getRight();

        if ((op == wang.yeting.sql.ast.expr.SQLBinaryOperator.BooleanAnd || op == wang.yeting.sql.ast.expr.SQLBinaryOperator.BooleanOr)
                && left instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr
                && ((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) left).getOperator() == op) {
            List<wang.yeting.sql.ast.SQLExpr> groupList = wang.yeting.sql.ast.expr.SQLBinaryOpExpr.split(x, op);
            for (int i = 0; i < groupList.size(); i++) {
                wang.yeting.sql.ast.SQLExpr item = groupList.get(i);
                item.accept(this);
            }
            return false;
        }

        switch (op) {
            case Equality:
            case NotEqual:
            case GreaterThan:
            case GreaterThanOrEqual:
            case LessThan:
            case LessThanOrGreater:
            case LessThanOrEqual:
            case LessThanOrEqualOrGreaterThan:
            case SoudsLike:
            case Like:
            case NotLike:
            case Is:
            case IsNot:
                handleCondition(left, op.name, right);

                String reverseOp = op.name;
                switch (op) {
                    case LessThan:
                        reverseOp = wang.yeting.sql.ast.expr.SQLBinaryOperator.GreaterThan.name;
                        break;
                    case LessThanOrEqual:
                        reverseOp = wang.yeting.sql.ast.expr.SQLBinaryOperator.GreaterThanOrEqual.name;
                        break;
                    case GreaterThan:
                        reverseOp = wang.yeting.sql.ast.expr.SQLBinaryOperator.LessThan.name;
                        break;
                    case GreaterThanOrEqual:
                        reverseOp = wang.yeting.sql.ast.expr.SQLBinaryOperator.LessThanOrEqual.name;
                        break;
                    default:
                        break;
                }
                handleCondition(right, reverseOp, left);


                handleRelationship(left, op.name, right);
                break;
            case BooleanOr: {
                List<wang.yeting.sql.ast.SQLExpr> list = wang.yeting.sql.ast.expr.SQLBinaryOpExpr.split(x, op);

                for (wang.yeting.sql.ast.SQLExpr item : list) {
                    if (item instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                        visit((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) item);
                    } else {
                        item.accept(this);
                    }
                }

                return false;
            }
            case Modulus:
                if (right instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    long hashCode64 = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) right).hashCode64();
                    if (hashCode64 == FnvHash.Constants.ISOPEN) {
                        left.accept(this);
                        return false;
                    }
                }
                break;
            default:
                break;
        }

        if (left instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
            visit((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) left);
        } else {
            statExpr(left);
        }
        statExpr(right);

        return false;
    }

    protected void handleRelationship(wang.yeting.sql.ast.SQLExpr left, String operator, wang.yeting.sql.ast.SQLExpr right) {
        Column leftColumn = getColumn(left);
        if (leftColumn == null) {
            return;
        }

        Column rightColumn = getColumn(right);
        if (rightColumn == null) {
            return;
        }

        Relationship relationship = new Relationship(leftColumn, rightColumn, operator);
        this.relationships.add(relationship);
    }

    protected void handleCondition(wang.yeting.sql.ast.SQLExpr expr, String operator, List<wang.yeting.sql.ast.SQLExpr> values) {
        handleCondition(expr, operator, values.toArray(new wang.yeting.sql.ast.SQLExpr[values.size()]));
    }

    protected void handleCondition(wang.yeting.sql.ast.SQLExpr expr, String operator, wang.yeting.sql.ast.SQLExpr... valueExprs) {
        if (expr instanceof wang.yeting.sql.ast.expr.SQLCastExpr) {
            expr = ((wang.yeting.sql.ast.expr.SQLCastExpr) expr).getExpr();
        } else if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
            wang.yeting.sql.ast.expr.SQLMethodInvokeExpr func = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
            List<wang.yeting.sql.ast.SQLExpr> arguments = func.getArguments();
            if (func.methodNameHashCode64() == FnvHash.Constants.COALESCE
                    && arguments.size() > 0) {
                boolean allLiteral = true;
                for (int i = 1; i < arguments.size(); ++i) {
                    wang.yeting.sql.ast.SQLExpr arg = arguments.get(i);
                    if (!(arg instanceof wang.yeting.sql.ast.expr.SQLLiteralExpr)) {
                        allLiteral = false;
                    }
                }
                if (allLiteral) {
                    expr = arguments.get(0);
                }
            }
        }

        Column column = getColumn(expr);

        if (column == null
                && expr instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr
                && valueExprs.length == 1 && valueExprs[0] instanceof wang.yeting.sql.ast.expr.SQLLiteralExpr) {
            wang.yeting.sql.ast.expr.SQLBinaryOpExpr left = (wang.yeting.sql.ast.expr.SQLBinaryOpExpr) expr;
            wang.yeting.sql.ast.expr.SQLLiteralExpr right = (wang.yeting.sql.ast.expr.SQLLiteralExpr) valueExprs[0];

            if (left.getRight() instanceof wang.yeting.sql.ast.expr.SQLIntegerExpr && right instanceof wang.yeting.sql.ast.expr.SQLIntegerExpr) {
                long v0 = ((wang.yeting.sql.ast.expr.SQLIntegerExpr) left.getRight()).getNumber().longValue();
                long v1 = ((wang.yeting.sql.ast.expr.SQLIntegerExpr) right).getNumber().longValue();

                wang.yeting.sql.ast.expr.SQLBinaryOperator op = left.getOperator();

                long v;
                switch (op) {
                    case Add:
                        v = v1 - v0;
                        break;
                    case Subtract:
                        v = v1 + v0;
                        break;
                    default:
                        return;
                }

                handleCondition(
                        left.getLeft(), operator, new wang.yeting.sql.ast.expr.SQLIntegerExpr(v));
                return;
            }
        }

        if (column == null) {
            return;
        }

        Condition condition = null;
        for (Condition item : this.getConditions()) {
            if (item.getColumn().equals(column) && item.getOperator().equals(operator)) {
                condition = item;
                break;
            }
        }

        if (condition == null) {
            condition = new Condition(column, operator);
            this.conditions.add(condition);
        }

        for (wang.yeting.sql.ast.SQLExpr item : valueExprs) {
            Column valueColumn = getColumn(item);
            if (valueColumn != null) {
                continue;
            }

            Object value;

            if (item instanceof wang.yeting.sql.ast.expr.SQLCastExpr) {
                item = ((wang.yeting.sql.ast.expr.SQLCastExpr) item).getExpr();
            }

            if (item instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr
                    || item instanceof wang.yeting.sql.ast.SQLCurrentTimeExpr) {
                value = item.toString();
            } else {
                value = SQLEvalVisitorUtils.eval(dbType, item, parameters, false);
                if (value == SQLEvalVisitor.EVAL_VALUE_NULL) {
                    value = null;
                }
            }

            condition.addValue(value);
        }
    }

    public DbType getDbType() {
        return dbType;
    }

    protected Column getColumn(wang.yeting.sql.ast.SQLExpr expr) {
        final wang.yeting.sql.ast.SQLExpr original = expr;

        // unwrap
        expr = unwrapExpr(expr);

        if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
            wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr;

            wang.yeting.sql.ast.SQLExpr owner = propertyExpr.getOwner();
            String column = SQLUtils.normalize(propertyExpr.getName());

            if (owner instanceof wang.yeting.sql.ast.SQLName) {
                wang.yeting.sql.ast.SQLName table = (wang.yeting.sql.ast.SQLName) owner;

                wang.yeting.sql.ast.SQLObject resolvedOwnerObject = propertyExpr.getResolvedOwnerObject();
                if (resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                        || resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLCreateProcedureStatement
                        || resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLCreateFunctionStatement) {
                    table = null;
                }

                if (resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    wang.yeting.sql.ast.SQLExpr tableSourceExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) resolvedOwnerObject).getExpr();
                    if (tableSourceExpr instanceof wang.yeting.sql.ast.SQLName) {
                        table = (wang.yeting.sql.ast.SQLName) tableSourceExpr;
                    }
                } else if (resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource) {
                    return null;
                }

                if (table != null) {
                    String tableName;
                    if (table instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                        tableName = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) table).normalizedName();
                    } else if (table instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                        tableName = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) table).normalizedName();
                    } else {
                        tableName = table.toString();
                    }

                    long tableHashCode64 = table.hashCode64();

                    if (resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                        SchemaObject schemaObject = ((wang.yeting.sql.ast.statement.SQLExprTableSource) resolvedOwnerObject).getSchemaObject();
                        if (schemaObject != null && schemaObject.getStatement() instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                            wang.yeting.sql.ast.statement.SQLColumnDefinition columnDef = schemaObject.findColumn(propertyExpr.nameHashCode64());
                            if (columnDef == null) {
                                tableName = "UNKNOWN";
                                tableHashCode64 = FnvHash.Constants.UNKNOWN;
                            }
                        }
                    }

                    long basic = tableHashCode64;
                    basic ^= '.';
                    basic *= FnvHash.PRIME;
                    long columnHashCode64 = FnvHash.hashCode64(basic, column);

                    Column columnObj = this.columns.get(columnHashCode64);
                    if (columnObj == null) {
                        columnObj = new Column(tableName, column, columnHashCode64);
                        if (!(resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                                || resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry)) {
                            this.columns.put(columnHashCode64, columnObj);
                        }
                    }

                    return columnObj;
                }
            }

            return null;
        }

        if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
            if (identifierExpr.getResolvedParameter() != null) {
                return null;
            }

            if (identifierExpr.getResolvedTableSource() instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
                return null;
            }

            if (identifierExpr.getResolvedDeclareItem() != null || identifierExpr.getResolvedParameter() != null) {
                return null;
            }

            String column = identifierExpr.getName();

            wang.yeting.sql.ast.SQLName table = null;
            wang.yeting.sql.ast.statement.SQLTableSource tableSource = identifierExpr.getResolvedTableSource();
            if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                wang.yeting.sql.ast.SQLExpr tableSourceExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();

                if (tableSourceExpr != null && !(tableSourceExpr instanceof wang.yeting.sql.ast.SQLName)) {
                    tableSourceExpr = unwrapExpr(tableSourceExpr);
                }

                if (tableSourceExpr instanceof wang.yeting.sql.ast.SQLName) {
                    table = (wang.yeting.sql.ast.SQLName) tableSourceExpr;
                }
            }

            if (table != null) {
                long tableHashCode64 = table.hashCode64();
                long basic = tableHashCode64;
                basic ^= '.';
                basic *= FnvHash.PRIME;
                long columnHashCode64 = FnvHash.hashCode64(basic, column);

                final Column old = columns.get(columnHashCode64);
                if (old != null) {
                    return old;
                }

                return new Column(table.toString(), column, columnHashCode64);
            }

            return new Column("UNKNOWN", column);
        }

        if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
            wang.yeting.sql.ast.expr.SQLMethodInvokeExpr methodInvokeExpr = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
            List<wang.yeting.sql.ast.SQLExpr> arguments = methodInvokeExpr.getArguments();
            long nameHash = methodInvokeExpr.methodNameHashCode64();
            if (nameHash == FnvHash.Constants.DATE_FORMAT) {
                if (arguments.size() == 2
                        && arguments.get(0) instanceof wang.yeting.sql.ast.SQLName
                        && arguments.get(1) instanceof wang.yeting.sql.ast.expr.SQLCharExpr) {
                    return getColumn(arguments.get(0));
                }
            }
        }

        return null;
    }

    private wang.yeting.sql.ast.SQLExpr unwrapExpr(wang.yeting.sql.ast.SQLExpr expr) {
        wang.yeting.sql.ast.SQLExpr original = expr;

        for (int i = 0; ; i++) {
            if (i > 1000) {
                return null;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr methodInvokeExp = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if (methodInvokeExp.getArguments().size() == 1) {
                    wang.yeting.sql.ast.SQLExpr firstExpr = methodInvokeExp.getArguments().get(0);
                    expr = firstExpr;
                    continue;
                }
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLCastExpr) {
                expr = ((wang.yeting.sql.ast.expr.SQLCastExpr) expr).getExpr();
                continue;
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr;

                wang.yeting.sql.ast.statement.SQLTableSource resolvedTableSource = propertyExpr.getResolvedTableSource();
                if (resolvedTableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
                    wang.yeting.sql.ast.statement.SQLSelect select = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) resolvedTableSource).getSelect();
                    wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = select.getFirstQueryBlock();
                    if (queryBlock != null) {
                        if (queryBlock.getGroupBy() != null) {
                            if (original.getParent() instanceof wang.yeting.sql.ast.expr.SQLBinaryOpExpr) {
                                wang.yeting.sql.ast.SQLExpr other = ((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) original.getParent()).other(original);
                                if (!wang.yeting.sql.ast.expr.SQLExprUtils.isLiteralExpr(other)) {
                                    break;
                                }
                            }
                        }

                        wang.yeting.sql.ast.statement.SQLSelectItem selectItem = queryBlock.findSelectItem(propertyExpr
                                .nameHashCode64());
                        if (selectItem != null) {
                            wang.yeting.sql.ast.SQLExpr selectItemExpr = selectItem.getExpr();
                            if (selectItemExpr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr
                                    && ((wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) selectItemExpr).getArguments().size() == 1
                            ) {
                                selectItemExpr = ((wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) selectItemExpr).getArguments().get(0);
                            }
                            if (selectItemExpr != expr) {
                                expr = selectItemExpr;
                                continue;
                            }
                        } else if (queryBlock.selectItemHasAllColumn()) {
                            wang.yeting.sql.ast.statement.SQLTableSource allColumnTableSource = null;

                            wang.yeting.sql.ast.statement.SQLTableSource from = queryBlock.getFrom();
                            if (from instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
                                wang.yeting.sql.ast.statement.SQLSelectItem allColumnSelectItem = queryBlock.findAllColumnSelectItem();
                                if (allColumnSelectItem != null && allColumnSelectItem.getExpr() instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                                    wang.yeting.sql.ast.SQLExpr owner = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) allColumnSelectItem.getExpr()).getOwner();
                                    if (owner instanceof wang.yeting.sql.ast.SQLName) {
                                        allColumnTableSource = from.findTableSource(((wang.yeting.sql.ast.SQLName) owner).nameHashCode64());
                                    }
                                }
                            } else {
                                allColumnTableSource = from;
                            }

                            if (allColumnTableSource == null) {
                                break;
                            }

                            propertyExpr = propertyExpr.clone();
                            propertyExpr.setResolvedTableSource(allColumnTableSource);

                            if (allColumnTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                                propertyExpr.setOwner(((wang.yeting.sql.ast.statement.SQLExprTableSource) allColumnTableSource).getExpr().clone());
                            }
                            expr = propertyExpr;
                            continue;
                        }
                    }
                } else if (resolvedTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    wang.yeting.sql.ast.statement.SQLExprTableSource exprTableSource = (wang.yeting.sql.ast.statement.SQLExprTableSource) resolvedTableSource;
                    if (exprTableSource.getSchemaObject() != null) {
                        break;
                    }

                    wang.yeting.sql.ast.statement.SQLTableSource redirectTableSource = null;
                    wang.yeting.sql.ast.SQLExpr tableSourceExpr = exprTableSource.getExpr();
                    if (tableSourceExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                        redirectTableSource = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) tableSourceExpr).getResolvedTableSource();
                    } else if (tableSourceExpr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                        redirectTableSource = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) tableSourceExpr).getResolvedTableSource();
                    }

                    if (redirectTableSource == resolvedTableSource) {
                        redirectTableSource = null;
                    }

                    if (redirectTableSource != null) {
                        propertyExpr = propertyExpr.clone();
                        if (redirectTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                            propertyExpr.setOwner(((wang.yeting.sql.ast.statement.SQLExprTableSource) redirectTableSource).getExpr().clone());
                        }
                        propertyExpr.setResolvedTableSource(redirectTableSource);
                        expr = propertyExpr;
                        continue;
                    }

                    propertyExpr = propertyExpr.clone();
                    propertyExpr.setOwner(tableSourceExpr);
                    expr = propertyExpr;
                    break;
                }
            }
            break;
        }

        return expr;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLTruncateStatement x) {
        setMode(x, Mode.Delete);

        for (wang.yeting.sql.ast.statement.SQLExprTableSource tableSource : x.getTableSources()) {
            wang.yeting.sql.ast.SQLName name = (wang.yeting.sql.ast.SQLName) tableSource.getExpr();
            TableStat stat = getTableStat(name);
            stat.incrementDeleteCount();
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropViewStatement x) {
        setMode(x, Mode.Drop);
        return true;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
        setMode(x, Mode.Insert);

        for (wang.yeting.sql.ast.statement.SQLExprTableSource tableSource : x.getTableSources()) {
            wang.yeting.sql.ast.SQLName name = (wang.yeting.sql.ast.SQLName) tableSource.getExpr();
            TableStat stat = getTableStat(name);
            stat.incrementDropCount();
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLInsertStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        setMode(x, Mode.Insert);

        if (x.getTableName() instanceof wang.yeting.sql.ast.SQLName) {
            String ident = ((wang.yeting.sql.ast.SQLName) x.getTableName()).toString();

            TableStat stat = getTableStat(x.getTableName());
            stat.incrementInsertCount();
        }

        accept(x.getColumns());
        accept(x.getQuery());

        return false;
    }

    protected void accept(wang.yeting.sql.ast.SQLObject x) {
        if (x != null) {
            x.accept(this);
        }
    }

    protected void accept(List<? extends wang.yeting.sql.ast.SQLObject> nodes) {
        for (int i = 0, size = nodes.size(); i < size; ++i) {
            accept(nodes.get(i));
        }
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLSelectQueryBlock x) {
        wang.yeting.sql.ast.statement.SQLTableSource from = x.getFrom();

        setMode(x, Mode.Select);

        boolean isHiveMultiInsert = false;
        if (from == null) {
            isHiveMultiInsert = x.getParent() != null
                    && x.getParent().getParent() instanceof HiveInsert
                    && x.getParent().getParent().getParent() instanceof HiveMultiInsertStatement;
            if (isHiveMultiInsert) {
                from = ((HiveMultiInsertStatement) x.getParent().getParent().getParent()).getFrom();
            }
        }

        if (from == null) {
            for (wang.yeting.sql.ast.statement.SQLSelectItem selectItem : x.getSelectList()) {
                statExpr(
                        selectItem.getExpr());
            }
            return false;
        }

//        if (x.getFrom() instanceof SQLSubqueryTableSource) {
//            x.getFrom().accept(this);
//            return false;
//        }

        if (from != null) {
            from.accept(this); // 提前执行，获得aliasMap
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource into = x.getInto();
        if (into != null && into.getExpr() instanceof wang.yeting.sql.ast.SQLName) {
            wang.yeting.sql.ast.SQLName intoExpr = (wang.yeting.sql.ast.SQLName) into.getExpr();

            boolean isParam = intoExpr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr && isParam((wang.yeting.sql.ast.expr.SQLIdentifierExpr) intoExpr);

            if (!isParam) {
                TableStat stat = getTableStat(intoExpr);
                if (stat != null) {
                    stat.incrementInsertCount();
                }
            }
            into.accept(this);
        }

        for (wang.yeting.sql.ast.statement.SQLSelectItem selectItem : x.getSelectList()) {
            if (selectItem.getClass() == wang.yeting.sql.ast.statement.SQLSelectItem.class) {
                statExpr(
                        selectItem.getExpr());
            } else {
                selectItem.accept(this);
            }
        }

        wang.yeting.sql.ast.SQLExpr where = x.getWhere();
        if (where != null) {
            statExpr(where);
        }

        wang.yeting.sql.ast.SQLExpr startWith = x.getStartWith();
        if (startWith != null) {
            statExpr(startWith);
        }

        wang.yeting.sql.ast.SQLExpr connectBy = x.getConnectBy();
        if (connectBy != null) {
            statExpr(connectBy);
        }

        wang.yeting.sql.ast.statement.SQLSelectGroupByClause groupBy = x.getGroupBy();
        if (groupBy != null) {
            for (wang.yeting.sql.ast.SQLExpr expr : groupBy.getItems()) {
                statExpr(expr);
            }
        }

        List<wang.yeting.sql.ast.SQLWindow> windows = x.getWindows();
        if (windows != null && windows.size() > 0) {
            for (wang.yeting.sql.ast.SQLWindow window : windows) {
                window.accept(this);
            }
        }

        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            this.visit(orderBy);
        }

        wang.yeting.sql.ast.SQLExpr first = x.getFirst();
        if (first != null) {
            statExpr(first);
        }

        List<wang.yeting.sql.ast.statement.SQLSelectOrderByItem> distributeBy = x.getDistributeBy();
        if (distributeBy != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : distributeBy) {
                statExpr(item.getExpr());
            }
        }

        List<wang.yeting.sql.ast.statement.SQLSelectOrderByItem> sortBy = x.getSortBy();
        if (sortBy != null) {
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem orderByItem : sortBy) {
                statExpr(orderByItem.getExpr());
            }
        }

        for (wang.yeting.sql.ast.SQLExpr expr : x.getForUpdateOf()) {
            statExpr(expr);
        }

        return false;
    }

    public void endVisit(wang.yeting.sql.ast.statement.SQLSelectQueryBlock x) {
        setModeOrigin(x);
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLJoinTableSource x) {
        wang.yeting.sql.ast.statement.SQLTableSource left = x.getLeft(), right = x.getRight();

        left.accept(this);
        right.accept(this);

        wang.yeting.sql.ast.SQLExpr condition = x.getCondition();
        if (condition != null) {
            condition.accept(this);
        }

        if (x.getUsing().size() > 0
                && left instanceof wang.yeting.sql.ast.statement.SQLExprTableSource && right instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr leftExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) left).getExpr();
            wang.yeting.sql.ast.SQLExpr rightExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) right).getExpr();

            for (wang.yeting.sql.ast.SQLExpr expr : x.getUsing()) {
                if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                    String name = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr).getName();
                    wang.yeting.sql.ast.expr.SQLPropertyExpr leftPropExpr = new wang.yeting.sql.ast.expr.SQLPropertyExpr(leftExpr, name);
                    wang.yeting.sql.ast.expr.SQLPropertyExpr rightPropExpr = new wang.yeting.sql.ast.expr.SQLPropertyExpr(rightExpr, name);

                    leftPropExpr.setResolvedTableSource(left);
                    rightPropExpr.setResolvedTableSource(right);

                    wang.yeting.sql.ast.expr.SQLBinaryOpExpr usingCondition = new wang.yeting.sql.ast.expr.SQLBinaryOpExpr(leftPropExpr, wang.yeting.sql.ast.expr.SQLBinaryOperator.Equality, rightPropExpr);
                    usingCondition.accept(this);
                }
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
        Column column = null;
        String ident = SQLUtils.normalize(x.getName());

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
            wang.yeting.sql.ast.statement.SQLSelect subSelect = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) tableSource).getSelect();
            wang.yeting.sql.ast.statement.SQLSelectQueryBlock subQuery = subSelect.getQueryBlock();
            if (subQuery != null) {
                wang.yeting.sql.ast.statement.SQLTableSource subTableSource = subQuery.findTableSourceWithColumn(x.nameHashCode64());
                if (subTableSource != null) {
                    tableSource = subTableSource;
                }
            }
        }

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr table = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
                wang.yeting.sql.ast.statement.SQLTableSource resolvedTableSource = table.getResolvedTableSource();
                if (resolvedTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) resolvedTableSource).getExpr();
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                wang.yeting.sql.ast.expr.SQLPropertyExpr table = (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr;
                wang.yeting.sql.ast.statement.SQLTableSource resolvedTableSource = table.getResolvedTableSource();
                if (resolvedTableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                    expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) resolvedTableSource).getExpr();
                }
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr table = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;

                wang.yeting.sql.ast.statement.SQLTableSource resolvedTableSource = table.getResolvedTableSource();
                if (resolvedTableSource instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) {
                    return false;
                }

                String tableName = table.getName();
                SchemaObject schemaObject = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getSchemaObject();
                if (schemaObject != null
                        && schemaObject.getStatement() instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement
                        && !"*".equals(ident)) {
                    wang.yeting.sql.ast.statement.SQLColumnDefinition columnDef = schemaObject.findColumn(x.nameHashCode64());
                    if (columnDef == null) {
                        column = addColumn("UNKNOWN", ident);
                    }
                }

                if (column == null) {
                    column = addColumn(table, ident);
                }

                if (isParentGroupBy(x)) {
                    this.groupByColumns.add(column);
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                wang.yeting.sql.ast.expr.SQLPropertyExpr table = (wang.yeting.sql.ast.expr.SQLPropertyExpr) expr;
                column = addColumn(table, ident);

                if (column != null && isParentGroupBy(x)) {
                    this.groupByColumns.add(column);
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr methodInvokeExpr = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if ("table".equalsIgnoreCase(methodInvokeExpr.getMethodName())
                        && methodInvokeExpr.getArguments().size() == 1
                        && methodInvokeExpr.getArguments().get(0) instanceof wang.yeting.sql.ast.SQLName) {
                    wang.yeting.sql.ast.SQLName table = (wang.yeting.sql.ast.SQLName) methodInvokeExpr.getArguments().get(0);

                    String tableName = null;
                    if (table instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                        wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = (wang.yeting.sql.ast.expr.SQLPropertyExpr) table;
                        wang.yeting.sql.ast.expr.SQLIdentifierExpr owner = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) propertyExpr.getOwner();
                        if (propertyExpr.getResolvedTableSource() != null
                                && propertyExpr.getResolvedTableSource() instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                            wang.yeting.sql.ast.SQLExpr resolveExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) propertyExpr.getResolvedTableSource()).getExpr();
                            if (resolveExpr instanceof wang.yeting.sql.ast.SQLName) {
                                tableName = resolveExpr.toString() + "." + propertyExpr.getName();
                            }
                        }
                    }

                    if (tableName == null) {
                        tableName = table.toString();
                    }

                    column = addColumn(tableName, ident);
                }
            }
        } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry
                || tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                || tableSource instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource
                || tableSource instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource
                || tableSource instanceof wang.yeting.sql.ast.statement.SQLLateralViewTableSource) {
            return false;
        } else {
            if (x.getResolvedProcudure() != null) {
                return false;
            }

            if (x.getResolvedOwnerObject() instanceof wang.yeting.sql.ast.SQLParameter) {
                return false;
            }

            boolean skip = false;
            for (wang.yeting.sql.ast.SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
                if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                    wang.yeting.sql.ast.statement.SQLTableSource from = ((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) parent).getFrom();

                    if (from instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource) {
                        skip = true;
                        break;
                    }
                } else if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQuery) {
                    break;
                }
            }
            if (!skip) {
                column = handleUnknownColumn(ident);
            }
        }

        if (column != null) {
            wang.yeting.sql.ast.SQLObject parent = x.getParent();
            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectOrderByItem) {
                parent = parent.getParent();
                if (parent instanceof wang.yeting.sql.ast.SQLIndexDefinition) {
                    parent = parent.getParent();
                }
            }
            if (parent instanceof wang.yeting.sql.ast.statement.SQLPrimaryKey) {
                column.setPrimaryKey(true);
            } else if (parent instanceof wang.yeting.sql.ast.statement.SQLUnique) {
                column.setUnique(true);
            }

            setColumn(x, column);
        }

        return false;
    }

    protected boolean isPseudoColumn(long hash) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
        if (isParam(x)) {
            return false;
        }

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();
        if (x.getParent() instanceof wang.yeting.sql.ast.statement.SQLSelectOrderByItem) {
            wang.yeting.sql.ast.statement.SQLSelectOrderByItem selectOrderByItem = (wang.yeting.sql.ast.statement.SQLSelectOrderByItem) x.getParent();
            if (selectOrderByItem.getResolvedSelectItem() != null) {
                return false;
            }
        }

        if (tableSource == null
                && (x.getResolvedParameter() != null
                || x.getResolvedDeclareItem() != null)) {
            return false;
        }

        long hash = x.nameHashCode64();
        if (isPseudoColumn(hash)) {
            return false;
        }

        if ((hash == FnvHash.Constants.LEVEL
                || hash == FnvHash.Constants.CONNECT_BY_ISCYCLE
                || hash == FnvHash.Constants.ROWNUM)
                && x.getResolvedColumn() == null
                && tableSource == null) {
            return false;
        }

        Column column = null;
        String ident = x.normalizedName();

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr expr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();

            if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr func = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if (func.methodNameHashCode64() == FnvHash.Constants.ANN) {
                    expr = func.getArguments().get(0);
                }
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr table = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
                column = addColumn(table, ident);

                if (column != null && isParentGroupBy(x)) {
                    this.groupByColumns.add(column);
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr || expr instanceof wang.yeting.sql.ast.expr.SQLDbLinkExpr) {
                String tableName;
                if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                    tableName = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) expr).normalizedName();
                } else {
                    tableName = expr.toString();
                }

                column = addColumn(tableName, ident);

                if (column != null && isParentGroupBy(x)) {
                    this.groupByColumns.add(column);
                }
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr methodInvokeExpr = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if ("table".equalsIgnoreCase(methodInvokeExpr.getMethodName())
                        && methodInvokeExpr.getArguments().size() == 1
                        && methodInvokeExpr.getArguments().get(0) instanceof wang.yeting.sql.ast.SQLName) {
                    wang.yeting.sql.ast.SQLName table = (wang.yeting.sql.ast.SQLName) methodInvokeExpr.getArguments().get(0);

                    String tableName = null;
                    if (table instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                        wang.yeting.sql.ast.expr.SQLPropertyExpr propertyExpr = (wang.yeting.sql.ast.expr.SQLPropertyExpr) table;
                        wang.yeting.sql.ast.expr.SQLIdentifierExpr owner = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) propertyExpr.getOwner();
                        if (propertyExpr.getResolvedTableSource() != null
                                && propertyExpr.getResolvedTableSource() instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
                            wang.yeting.sql.ast.SQLExpr resolveExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) propertyExpr.getResolvedTableSource()).getExpr();
                            if (resolveExpr instanceof wang.yeting.sql.ast.SQLName) {
                                tableName = resolveExpr.toString() + "." + propertyExpr.getName();
                            }
                        }
                    }

                    if (tableName == null) {
                        tableName = table.toString();
                    }

                    column = addColumn(tableName, ident);
                }
            }
        } else if (tableSource instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry
                || tableSource instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                || tableSource instanceof wang.yeting.sql.ast.statement.SQLLateralViewTableSource) {
            return false;
        } else {
            boolean skip = false;
            for (wang.yeting.sql.ast.SQLObject parent = x.getParent(); parent != null; parent = parent.getParent()) {
                if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                    wang.yeting.sql.ast.statement.SQLTableSource from = ((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) parent).getFrom();

                    if (from instanceof wang.yeting.sql.ast.statement.SQLValuesTableSource) {
                        skip = true;
                        break;
                    }
                } else if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQuery) {
                    break;
                }
            }
            if (x.getParent() instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr
                    && ((wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) x.getParent()).methodNameHashCode64() == FnvHash.Constants.ANN) {
                skip = true;
            }

            if (!skip) {
                column = handleUnknownColumn(ident);
            }
        }

        if (column != null) {
            wang.yeting.sql.ast.SQLObject parent = x.getParent();
            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectOrderByItem) {
                parent = parent.getParent();
                if (parent instanceof wang.yeting.sql.ast.SQLIndexDefinition) {
                    parent = parent.getParent();
                }
            }
            if (parent instanceof wang.yeting.sql.ast.statement.SQLPrimaryKey) {
                column.setPrimaryKey(true);
            } else if (parent instanceof wang.yeting.sql.ast.statement.SQLUnique) {
                column.setUnique(true);
            }

            setColumn(x, column);
        }

        return false;
    }

    private boolean isParentSelectItem(wang.yeting.sql.ast.SQLObject parent) {
        for (int i = 0; parent != null; parent = parent.getParent(), ++i) {
            if (i > 100) {
                break;
            }

            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectItem) {
                return true;
            }

            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                return false;
            }
        }
        return false;
    }

    private boolean isParentGroupBy(wang.yeting.sql.ast.SQLObject parent) {
        for (; parent != null; parent = parent.getParent()) {
            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectItem) {
                return false;
            }

            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectGroupByClause) {
                return true;
            }
        }
        return false;
    }

    private void setColumn(wang.yeting.sql.ast.SQLExpr x, Column column) {
        wang.yeting.sql.ast.SQLObject current = x;
        for (int i = 0; i < 100; ++i) {
            wang.yeting.sql.ast.SQLObject parent = current.getParent();

            if (parent == null) {
                break;
            }

            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                wang.yeting.sql.ast.statement.SQLSelectQueryBlock query = (wang.yeting.sql.ast.statement.SQLSelectQueryBlock) parent;
                if (query.getWhere() == current) {
                    column.setWhere(true);
                }
                break;
            }

            if (parent instanceof wang.yeting.sql.ast.statement.SQLSelectGroupByClause) {
                wang.yeting.sql.ast.statement.SQLSelectGroupByClause groupBy = (wang.yeting.sql.ast.statement.SQLSelectGroupByClause) parent;
                if (current == groupBy.getHaving()) {
                    column.setHaving(true);
                } else if (groupBy.getItems().contains(current)) {
                    column.setGroupBy(true);
                }
                break;
            }

            if (isParentSelectItem(parent)) {
                column.setSelec(true);
                break;
            }

            if (parent instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
                wang.yeting.sql.ast.statement.SQLJoinTableSource join = (wang.yeting.sql.ast.statement.SQLJoinTableSource) parent;
                if (join.getCondition() == current) {
                    column.setJoin(true);
                }
                break;
            }

            current = parent;
        }
    }

    protected Column handleUnknownColumn(String columnName) {
        return addColumn("UNKNOWN", columnName);
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLAllColumnExpr x) {
        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();
        if (tableSource == null) {
            return false;
        }

        statAllColumn(x, tableSource);

        return false;
    }

    private void statAllColumn(wang.yeting.sql.ast.expr.SQLAllColumnExpr x, wang.yeting.sql.ast.statement.SQLTableSource tableSource) {
        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            statAllColumn(x, (wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource);
            return;
        }

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLJoinTableSource) {
            wang.yeting.sql.ast.statement.SQLJoinTableSource join = (wang.yeting.sql.ast.statement.SQLJoinTableSource) tableSource;
            statAllColumn(x, join.getLeft());
            statAllColumn(x, join.getRight());
        }
    }

    private void statAllColumn(wang.yeting.sql.ast.expr.SQLAllColumnExpr x, wang.yeting.sql.ast.statement.SQLExprTableSource tableSource) {
        wang.yeting.sql.ast.statement.SQLExprTableSource exprTableSource = tableSource;
        wang.yeting.sql.ast.SQLName expr = exprTableSource.getName();

        wang.yeting.sql.ast.statement.SQLCreateTableStatement createStmt = null;

        SchemaObject tableObject = exprTableSource.getSchemaObject();
        if (tableObject != null) {
            wang.yeting.sql.ast.SQLStatement stmt = tableObject.getStatement();
            if (stmt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                createStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) stmt;
            }
        }

        if (createStmt != null
                && createStmt.getTableElementList().size() > 0) {
            wang.yeting.sql.ast.SQLName tableName = createStmt.getName();
            for (wang.yeting.sql.ast.statement.SQLTableElement e : createStmt.getTableElementList()) {
                if (e instanceof wang.yeting.sql.ast.statement.SQLColumnDefinition) {
                    wang.yeting.sql.ast.statement.SQLColumnDefinition columnDefinition = (wang.yeting.sql.ast.statement.SQLColumnDefinition) e;
                    wang.yeting.sql.ast.SQLName columnName = columnDefinition.getName();
                    Column column = addColumn(tableName, columnName.toString());
                    if (isParentSelectItem(x.getParent())) {
                        column.setSelec(true);
                    }
                }
            }
        } else if (expr != null) {
            Column column = addColumn(expr, "*");
            if (isParentSelectItem(x.getParent())) {
                column.setSelec(true);
            }
        }
    }

    public Map<TableStat.Name, TableStat> getTables() {
        return tableStats;
    }

    public boolean containsTable(String tableName) {
        return tableStats.containsKey(new TableStat.Name(tableName));
    }

    public boolean containsColumn(String tableName, String columnName) {
        long hashCode;

        int p = tableName.indexOf('.');
        if (p != -1) {
            wang.yeting.sql.ast.SQLExpr owner = SQLUtils.toSQLExpr(tableName, dbType);
            hashCode = new wang.yeting.sql.ast.expr.SQLPropertyExpr(owner, columnName).hashCode64();
        } else {
            hashCode = FnvHash.hashCode64(tableName, columnName);
        }
        return columns.containsKey(hashCode);
    }

    public Collection<Column> getColumns() {
        return columns.values();
    }

    public Column getColumn(String tableName, String columnName) {
        Column column = new Column(tableName, columnName);

        return this.columns.get(column.hashCode64());
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLSelectStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        visit(x.getSelect());

        return false;
    }

    public void endVisit(wang.yeting.sql.ast.statement.SQLSelectStatement x) {
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry x) {
        String alias = x.getAlias();
        wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = (wang.yeting.sql.ast.statement.SQLWithSubqueryClause) x.getParent();

        if (Boolean.TRUE == with.getRecursive()) {
            wang.yeting.sql.ast.statement.SQLSelect select = x.getSubQuery();
            if (select != null) {
                select.accept(this);
            } else {
                x.getReturningStatement().accept(this);
            }
        } else {
            wang.yeting.sql.ast.statement.SQLSelect select = x.getSubQuery();
            if (select != null) {
                select.accept(this);
            } else {
                x.getReturningStatement().accept(this);
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLSubqueryTableSource x) {
        x.getSelect().accept(this);
        return false;
    }

    protected boolean isSimpleExprTableSource(wang.yeting.sql.ast.statement.SQLExprTableSource x) {
        return x.getExpr() instanceof wang.yeting.sql.ast.SQLName;
    }

    public TableStat getTableStat(wang.yeting.sql.ast.statement.SQLExprTableSource tableSource) {
        return getTableStatWithUnwrap(
                tableSource.getExpr());
    }

    protected TableStat getTableStatWithUnwrap(wang.yeting.sql.ast.SQLExpr expr) {
        wang.yeting.sql.ast.SQLExpr identExpr = null;

        expr = unwrapExpr(expr);

        if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;

            if (identifierExpr.nameHashCode64() == FnvHash.Constants.DUAL) {
                return null;
            }

            if (isSubQueryOrParamOrVariant(identifierExpr)) {
                return null;
            }
        }

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = null;
        if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            tableSource = ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr).getResolvedTableSource();
        } else if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
            tableSource = ((wang.yeting.sql.ast.expr.SQLPropertyExpr) expr).getResolvedTableSource();
        }

        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLExpr tableSourceExpr = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getExpr();
            if (tableSourceExpr instanceof wang.yeting.sql.ast.SQLName) {
                identExpr = tableSourceExpr;
            }
        }

        if (identExpr == null) {
            identExpr = expr;
        }

        if (identExpr instanceof wang.yeting.sql.ast.SQLName) {
            return getTableStat((wang.yeting.sql.ast.SQLName) identExpr);
        }
        return getTableStat(identExpr.toString());
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLExprTableSource x) {
        wang.yeting.sql.ast.SQLExpr expr = x.getExpr();
        if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
            wang.yeting.sql.ast.expr.SQLMethodInvokeExpr func = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
            if (func.methodNameHashCode64() == FnvHash.Constants.ANN) {
                expr = func.getArguments().get(0);
            }
        }

        if (isSimpleExprTableSource(x)) {
            TableStat stat = getTableStatWithUnwrap(expr);
            if (stat == null) {
                return false;
            }

            Mode mode = getMode();
            if (mode != null) {
                switch (mode) {
                    case Delete:
                        stat.incrementDeleteCount();
                        break;
                    case Insert:
                        stat.incrementInsertCount();
                        break;
                    case Update:
                        stat.incrementUpdateCount();
                        break;
                    case Select:
                        stat.incrementSelectCount();
                        break;
                    case Merge:
                        stat.incrementMergeCount();
                        break;
                    case Drop:
                        stat.incrementDropCount();
                        break;
                    default:
                        break;
                }
            }
        } else {
            accept(expr);
        }

        return false;
    }

    protected boolean isSubQueryOrParamOrVariant(wang.yeting.sql.ast.expr.SQLIdentifierExpr identifierExpr) {
        wang.yeting.sql.ast.SQLObject resolvedColumnObject = identifierExpr.getResolvedColumnObject();
        if (resolvedColumnObject instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry
                || resolvedColumnObject instanceof wang.yeting.sql.ast.SQLParameter
                || resolvedColumnObject instanceof wang.yeting.sql.ast.SQLDeclareItem) {
            return true;
        }

        wang.yeting.sql.ast.SQLObject resolvedOwnerObject = identifierExpr.getResolvedOwnerObject();
        if (resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                || resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) {
            return true;
        }

        return false;
    }

    protected boolean isSubQueryOrParamOrVariant(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
        wang.yeting.sql.ast.SQLObject resolvedOwnerObject = x.getResolvedOwnerObject();
        if (resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource
                || resolvedOwnerObject instanceof wang.yeting.sql.ast.statement.SQLWithSubqueryClause.Entry) {
            return true;
        }

        wang.yeting.sql.ast.SQLExpr owner = x.getOwner();
        if (owner instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            if (isSubQueryOrParamOrVariant((wang.yeting.sql.ast.expr.SQLIdentifierExpr) owner)) {
                return true;
            }
        }

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getResolvedTableSource();
        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.statement.SQLExprTableSource exprTableSource = (wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource;
            if (exprTableSource.getSchemaObject() != null) {
                return false;
            }

            wang.yeting.sql.ast.SQLExpr expr = exprTableSource.getExpr();

            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                return isSubQueryOrParamOrVariant((wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr);
            }

            if (expr instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                return isSubQueryOrParamOrVariant((wang.yeting.sql.ast.expr.SQLPropertyExpr) expr);
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLSelectItem x) {
        statExpr(
                x.getExpr());

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLSelect x) {
        wang.yeting.sql.ast.statement.SQLWithSubqueryClause with = x.getWithSubQuery();
        if (with != null) {
            with.accept(this);
        }

        wang.yeting.sql.ast.statement.SQLSelectQuery query = x.getQuery();
        if (query != null) {
            query.accept(this);
        }

        wang.yeting.sql.ast.SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            accept(x.getOrderBy());
        }


        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLAggregateExpr x) {
        this.aggregateFunctions.add(x);

        accept(x.getArguments());
        accept(x.getOrderBy());
        accept(x.getOver());
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLMethodInvokeExpr x) {
        this.functions.add(x);

        accept(x.getArguments());
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLUpdateStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        setMode(x, Mode.Update);

        wang.yeting.sql.ast.statement.SQLTableSource tableSource = x.getTableSource();
        if (tableSource instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            wang.yeting.sql.ast.SQLName identName = ((wang.yeting.sql.ast.statement.SQLExprTableSource) tableSource).getName();
            TableStat stat = getTableStat(identName);
            stat.incrementUpdateCount();
        } else {
            tableSource.accept(this);
        }

        final wang.yeting.sql.ast.statement.SQLTableSource from = x.getFrom();
        if (from != null) {
            from.accept(this);
        }

        final List<wang.yeting.sql.ast.statement.SQLUpdateSetItem> items = x.getItems();
        for (int i = 0, size = items.size(); i < size; ++i) {
            wang.yeting.sql.ast.statement.SQLUpdateSetItem item = items.get(i);
            visit(item);
        }

        final wang.yeting.sql.ast.SQLExpr where = x.getWhere();
        if (where != null) {
            where.accept(this);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLUpdateSetItem x) {
        final wang.yeting.sql.ast.SQLExpr column = x.getColumn();
        if (column != null) {
            statExpr(column);

            final Column columnStat = getColumn(column);
            if (columnStat != null) {
                columnStat.setUpdate(true);
            }
        }

        final wang.yeting.sql.ast.SQLExpr value = x.getValue();
        if (value != null) {
            statExpr(value);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDeleteStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        setMode(x, Mode.Delete);

        if (x.getTableSource() instanceof wang.yeting.sql.ast.statement.SQLSubqueryTableSource) {
            wang.yeting.sql.ast.statement.SQLSelectQuery selectQuery = ((wang.yeting.sql.ast.statement.SQLSubqueryTableSource) x.getTableSource()).getSelect().getQuery();
            if (selectQuery instanceof wang.yeting.sql.ast.statement.SQLSelectQueryBlock) {
                wang.yeting.sql.ast.statement.SQLSelectQueryBlock subQueryBlock = ((wang.yeting.sql.ast.statement.SQLSelectQueryBlock) selectQuery);
                subQueryBlock.getWhere().accept(this);
            }
        }

        TableStat stat = getTableStat(x.getTableName());
        stat.incrementDeleteCount();

        final wang.yeting.sql.ast.SQLExpr where = x.getWhere();
        if (where != null) {
            where.accept(this);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLInListExpr x) {
        if (x.isNot()) {
            handleCondition(x.getExpr(), "NOT IN", x.getTargetList());
        } else {
            handleCondition(x.getExpr(), "IN", x.getTargetList());
        }

        return true;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLInSubQueryExpr x) {
        if (x.isNot()) {
            handleCondition(x.getExpr(), "NOT IN");
        } else {
            handleCondition(x.getExpr(), "IN");
        }
        return true;
    }

    public void endVisit(wang.yeting.sql.ast.statement.SQLDeleteStatement x) {

    }

    public void endVisit(wang.yeting.sql.ast.statement.SQLUpdateStatement x) {
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        for (wang.yeting.sql.ast.statement.SQLTableElement e : x.getTableElementList()) {
            e.setParent(x);
        }

        TableStat stat = getTableStat(x.getName());
        stat.incrementCreateCount();

        accept(x.getTableElementList());

        if (x.getInherits() != null) {
            x.getInherits().accept(this);
        }

        if (x.getSelect() != null) {
            x.getSelect().accept(this);
            stat.incrementInsertCount();
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource like = x.getLike();
        if (like != null) {
            like.accept(this);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLColumnDefinition x) {
        wang.yeting.sql.ast.SQLName tableName = null;
        {
            wang.yeting.sql.ast.SQLObject parent = x.getParent();
            if (parent instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                tableName = ((wang.yeting.sql.ast.statement.SQLCreateTableStatement) parent).getName();
            }
        }

        if (tableName == null) {
            return true;
        }

        String columnName = x.getName().toString();
        Column column = addColumn(tableName, columnName);
        if (x.getDataType() != null) {
            column.setDataType(x.getDataType().getName());
        }

        for (wang.yeting.sql.ast.statement.SQLColumnConstraint item : x.getConstraints()) {
            if (item instanceof wang.yeting.sql.ast.statement.SQLPrimaryKey) {
                column.setPrimaryKey(true);
            } else if (item instanceof wang.yeting.sql.ast.statement.SQLUnique) {
                column.setUnique(true);
            }
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCallStatement x) {
        return false;
    }

    @Override
    public void endVisit(wang.yeting.sql.ast.statement.SQLCommentStatement x) {

    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCommentStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLCurrentOfCursorExpr x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddColumn x) {
        wang.yeting.sql.ast.statement.SQLAlterTableStatement stmt = (wang.yeting.sql.ast.statement.SQLAlterTableStatement) x.getParent();
        String table = stmt.getName().toString();

        for (wang.yeting.sql.ast.statement.SQLColumnDefinition column : x.getColumns()) {
            String columnName = SQLUtils.normalize(column.getName().toString());
            addColumn(stmt.getName(), columnName);
        }
        return false;
    }

    @Override
    public void endVisit(wang.yeting.sql.ast.statement.SQLAlterTableAddColumn x) {

    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLRollbackStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        wang.yeting.sql.ast.statement.SQLSelect subQuery = x.getSubQuery();
        if (subQuery != null) {
            subQuery.accept(this);
        }

        wang.yeting.sql.ast.statement.SQLBlockStatement script = x.getScript();
        if (script != null) {
            script.accept(this);
        }
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        x.getSubQuery().accept(this);
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropForeignKey x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLUseStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDisableConstraint x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableEnableConstraint x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        TableStat stat = getTableStat(x.getName());
        stat.incrementAlterCount();


        for (wang.yeting.sql.ast.statement.SQLAlterTableItem item : x.getItems()) {
            item.setParent(x);
            if (item instanceof wang.yeting.sql.ast.statement.SQLAlterTableAddPartition
                    || item instanceof wang.yeting.sql.ast.statement.SQLAlterTableRenamePartition
                    || item instanceof wang.yeting.sql.ast.statement.SQLAlterTableMergePartition
            ) {
                stat.incrementAddPartitionCount();
            }

            item.accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropConstraint x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
        setMode(x, Mode.DropIndex);
        wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTableName();
        if (table != null) {
            wang.yeting.sql.ast.SQLName name = (wang.yeting.sql.ast.SQLName) table.getExpr();
            TableStat stat = getTableStat(name);
            stat.incrementDropIndexCount();
        }
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {
        setMode(x, Mode.CreateIndex);

        wang.yeting.sql.ast.SQLName table = (wang.yeting.sql.ast.SQLName) ((wang.yeting.sql.ast.statement.SQLExprTableSource) x.getTable()).getExpr();
        TableStat stat = getTableStat(table);
        stat.incrementCreateIndexCount();

        for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : x.getItems()) {
            wang.yeting.sql.ast.SQLExpr expr = item.getExpr();
            if (expr instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr identExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) expr;
                String columnName = identExpr.getName();
                addColumn(table, columnName);
            } else if (expr instanceof wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) {
                wang.yeting.sql.ast.expr.SQLMethodInvokeExpr methodInvokeExpr = (wang.yeting.sql.ast.expr.SQLMethodInvokeExpr) expr;
                if (methodInvokeExpr.getArguments().size() == 1) {
                    wang.yeting.sql.ast.SQLExpr param = methodInvokeExpr.getArguments().get(0);
                    if (param instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                        wang.yeting.sql.ast.expr.SQLIdentifierExpr identExpr = (wang.yeting.sql.ast.expr.SQLIdentifierExpr) param;
                        String columnName = identExpr.getName();
                        addColumn(table, columnName);
                    }
                }
            }
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLForeignKeyImpl x) {

        for (wang.yeting.sql.ast.SQLName column : x.getReferencingColumns()) {
            column.accept(this);
        }

        wang.yeting.sql.ast.SQLName table = x.getReferencedTableName();

        TableStat stat = getTableStat(x.getReferencedTableName());
        stat.incrementReferencedCount();
        for (wang.yeting.sql.ast.SQLName column : x.getReferencedColumns()) {
            String columnName = column.getSimpleName();
            addColumn(table, columnName);
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropTriggerStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropUserStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLGrantStatement x) {
        if (x.getResource() != null && (x.getResourceType() == null || x.getResourceType() == wang.yeting.sql.ast.statement.SQLObjectType.TABLE)) {
            x.getResource().accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLRevokeStatement x) {
        if (x.getResource() != null) {
            x.getResource().accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropDatabaseStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddIndex x) {
        for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : x.getColumns()) {
            item.accept(this);
        }

        wang.yeting.sql.ast.SQLName table = ((wang.yeting.sql.ast.statement.SQLAlterTableStatement) x.getParent()).getName();
        TableStat tableStat = this.getTableStat(table);
        tableStat.incrementCreateIndexCount();
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCheck x) {
        x.getExpr().accept(this);
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDefault x) {
        x.getExpr().accept(this);
        x.getColumn().accept(this);
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateTriggerStatement x) {
        wang.yeting.sql.ast.statement.SQLExprTableSource on = x.getOn();
        on.accept(this);
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropFunctionStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropTableSpaceStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropProcedureStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRename x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.expr.SQLArrayExpr x) {
        accept(x.getValues());

        wang.yeting.sql.ast.SQLExpr exp = x.getExpr();
        if (exp instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
            if (((wang.yeting.sql.ast.expr.SQLIdentifierExpr) exp).getName().equals("ARRAY")) {
                return false;
            }
        }
        if (exp != null) {
            exp.accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLOpenStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLFetchStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLShowMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLRefreshMaterializedViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCloseStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateProcedureStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        accept(x.getBlock());
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        accept(x.getBlock());
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLBlockStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        for (wang.yeting.sql.ast.SQLParameter param : x.getParameters()) {
            param.setParent(x);
            param.accept(this);
        }

        for (wang.yeting.sql.ast.SQLStatement stmt : x.getStatementList()) {
            stmt.accept(this);
        }

        wang.yeting.sql.ast.SQLStatement exception = x.getException();
        if (exception != null) {
            exception.accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLShowTablesStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLDeclareItem x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLPartitionByHash x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLPartitionByRange x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLPartitionByList x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLSubPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLSubPartitionByHash x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.SQLPartitionValue x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterDatabaseStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableConvertCharSet x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableReOrganizePartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableCoalescePartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableTruncatePartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDiscardPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableImportPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAnalyzePartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableCheckPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableOptimizePartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRebuildPartition x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableRepairPartition x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.expr.SQLSequenceExpr x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLMergeStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        setMode(x.getUsing(), Mode.Select);
        x.getUsing().accept(this);

        setMode(x, Mode.Merge);

        wang.yeting.sql.ast.statement.SQLTableSource into = x.getInto();
        if (into instanceof wang.yeting.sql.ast.statement.SQLExprTableSource) {
            String ident = ((wang.yeting.sql.ast.statement.SQLExprTableSource) into).getExpr().toString();
            TableStat stat = getTableStat(ident);
            stat.incrementMergeCount();
        } else {
            into.accept(this);
        }

        x.getOn().accept(this);

        if (x.getUpdateClause() != null) {
            x.getUpdateClause().accept(this);
        }

        if (x.getInsertClause() != null) {
            x.getInsertClause().accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLSetStatement x) {
        return false;
    }

    public List<wang.yeting.sql.ast.expr.SQLMethodInvokeExpr> getFunctions() {
        return this.functions;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableAddConstraint x) {
        wang.yeting.sql.ast.statement.SQLConstraint constraint = x.getConstraint();
        if (constraint instanceof wang.yeting.sql.ast.statement.SQLUniqueConstraint) {
            wang.yeting.sql.ast.statement.SQLAlterTableStatement stmt = (wang.yeting.sql.ast.statement.SQLAlterTableStatement) x.getParent();
            TableStat tableStat = this.getTableStat(stmt.getName());
            tableStat.incrementCreateIndexCount();
        }
        return true;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropIndex x) {
        wang.yeting.sql.ast.statement.SQLAlterTableStatement stmt = (wang.yeting.sql.ast.statement.SQLAlterTableStatement) x.getParent();
        TableStat tableStat = this.getTableStat(stmt.getName());
        tableStat.incrementDropIndexCount();
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropPrimaryKey x) {
        wang.yeting.sql.ast.statement.SQLAlterTableStatement stmt = (wang.yeting.sql.ast.statement.SQLAlterTableStatement) x.getParent();
        TableStat tableStat = this.getTableStat(stmt.getName());
        tableStat.incrementDropIndexCount();
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableDropKey x) {
        wang.yeting.sql.ast.statement.SQLAlterTableStatement stmt = (wang.yeting.sql.ast.statement.SQLAlterTableStatement) x.getParent();
        TableStat tableStat = this.getTableStat(stmt.getName());
        tableStat.incrementDropIndexCount();
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDescribeStatement x) {
        wang.yeting.sql.ast.SQLName tableName = x.getObject();

        TableStat tableStat = this.getTableStat(x.getObject());

        wang.yeting.sql.ast.SQLName column = x.getColumn();
        if (column != null) {
            String columnName = column.toString();
            this.addColumn(tableName, columnName);
        }
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLExplainStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        if (x.getStatement() != null) {
            accept(x.getStatement());
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateMaterializedViewStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        wang.yeting.sql.ast.statement.SQLSelect query = x.getQuery();
        if (query != null) {
            query.accept(this);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLReplaceStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        setMode(x, Mode.Replace);

        wang.yeting.sql.ast.SQLName tableName = x.getTableName();

        TableStat stat = getTableStat(tableName);

        if (stat != null) {
            stat.incrementInsertCount();
        }

        accept(x.getColumns());
        accept(x.getValuesList());
        accept(x.getQuery());

        return false;
    }

    protected final void statExpr(wang.yeting.sql.ast.SQLExpr x) {
        if (x == null) {
            return;
        }

        Class<?> clazz = x.getClass();
        if (clazz == wang.yeting.sql.ast.expr.SQLIdentifierExpr.class) {
            visit((wang.yeting.sql.ast.expr.SQLIdentifierExpr) x);
        } else if (clazz == wang.yeting.sql.ast.expr.SQLPropertyExpr.class) {
            visit((wang.yeting.sql.ast.expr.SQLPropertyExpr) x);
//        } else if (clazz == SQLAggregateExpr.class) {
//            visit((SQLAggregateExpr) x);
        } else if (clazz == wang.yeting.sql.ast.expr.SQLBinaryOpExpr.class) {
            visit((wang.yeting.sql.ast.expr.SQLBinaryOpExpr) x);
//        } else if (clazz == SQLCharExpr.class) {
//            visit((SQLCharExpr) x);
//        } else if (clazz == SQLNullExpr.class) {
//            visit((SQLNullExpr) x);
//        } else if (clazz == SQLIntegerExpr.class) {
//            visit((SQLIntegerExpr) x);
//        } else if (clazz == SQLNumberExpr.class) {
//            visit((SQLNumberExpr) x);
//        } else if (clazz == SQLMethodInvokeExpr.class) {
//            visit((SQLMethodInvokeExpr) x);
//        } else if (clazz == SQLVariantRefExpr.class) {
//            visit((SQLVariantRefExpr) x);
//        } else if (clazz == SQLBinaryOpExprGroup.class) {
//            visit((SQLBinaryOpExprGroup) x);
        } else if (x instanceof wang.yeting.sql.ast.expr.SQLLiteralExpr) {
            // skip
        } else {
            x.accept(this);
        }
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterFunctionStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropSynonymStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTypeStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterProcedureStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLExprStatement x) {
        wang.yeting.sql.ast.SQLExpr expr = x.getExpr();

        if (expr instanceof wang.yeting.sql.ast.SQLName) {
            return false;
        }

        return true;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropTypeStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLExternalRecordFormat x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateDatabaseStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableGroupStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropTableGroupStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLShowDatabasesStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLShowColumnsStatement x) {
        wang.yeting.sql.ast.SQLName table = x.getTable();

        TableStat stat = getTableStat(table);
        if (stat != null) {
//            stat.incrementSh
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLShowCreateTableStatement x) {
        wang.yeting.sql.ast.SQLName table = x.getName();

        if (table != null) {
            TableStat stat = getTableStat(table);
            if (stat != null) {
//            stat.incrementSh
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLShowTableGroupsStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableSetOption x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLShowCreateViewStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateRoleStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLDropRoleStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLShowViewsStatement x) {
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableExchangePartition x) {
        wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTable();
        if (table != null) {
            table.accept(this);
        }
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropCatalogStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLUnionQuery x) {
        wang.yeting.sql.ast.statement.SQLUnionOperator operator = x.getOperator();
        List<wang.yeting.sql.ast.statement.SQLSelectQuery> relations = x.getRelations();
        if (relations.size() > 2) {
            for (wang.yeting.sql.ast.statement.SQLSelectQuery relation : x.getRelations()) {
                relation.accept(this);
            }
            return false;
        }

        wang.yeting.sql.ast.statement.SQLSelectQuery left = x.getLeft();
        wang.yeting.sql.ast.statement.SQLSelectQuery right = x.getRight();

        boolean bracket = x.isParenthesized() && !(x.getParent() instanceof wang.yeting.sql.ast.statement.SQLUnionQueryTableSource);

        if ((!bracket)
                && left instanceof wang.yeting.sql.ast.statement.SQLUnionQuery
                && ((wang.yeting.sql.ast.statement.SQLUnionQuery) left).getOperator() == operator
                && !right.isParenthesized()
                && x.getOrderBy() == null) {

            wang.yeting.sql.ast.statement.SQLUnionQuery leftUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) left;

            List<wang.yeting.sql.ast.statement.SQLSelectQuery> rights = new ArrayList<wang.yeting.sql.ast.statement.SQLSelectQuery>();
            rights.add(right);

            if (leftUnion.getRelations().size() > 2) {
                rights.addAll(leftUnion.getRelations());
            } else {
                for (; ; ) {
                    wang.yeting.sql.ast.statement.SQLSelectQuery leftLeft = leftUnion.getLeft();
                    wang.yeting.sql.ast.statement.SQLSelectQuery leftRight = leftUnion.getRight();

                    if ((!leftUnion.isParenthesized())
                            && leftUnion.getOrderBy() == null
                            && (!leftLeft.isParenthesized())
                            && (!leftRight.isParenthesized())
                            && leftLeft instanceof wang.yeting.sql.ast.statement.SQLUnionQuery
                            && ((wang.yeting.sql.ast.statement.SQLUnionQuery) leftLeft).getOperator() == operator) {
                        rights.add(leftRight);
                        leftUnion = (wang.yeting.sql.ast.statement.SQLUnionQuery) leftLeft;
                        continue;
                    } else {
                        rights.add(leftRight);
                        rights.add(leftLeft);
                    }
                    break;
                }
            }

            for (int i = rights.size() - 1; i >= 0; i--) {
                wang.yeting.sql.ast.statement.SQLSelectQuery item = rights.get(i);
                item.accept(this);
            }
            return false;
        }

        return true;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLValuesTableSource x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterIndexStatement x) {
        final wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTable();
        if (table != null) {
            table.accept(this);
        }
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLShowIndexesStatement x) {
        final wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTable();
        if (table != null) {
            table.accept(this);
        }
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAnalyzeTableStatement x) {
        for (wang.yeting.sql.ast.statement.SQLExprTableSource table : x.getTables()) {
            if (table != null) {
                TableStat stat = getTableStat(table.getName());
                if (stat != null) {
                    stat.incrementAnalyzeCount();
                }
            }
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTables().size() == 1 ? x.getTables().get(0) : null;

        wang.yeting.sql.ast.statement.SQLPartitionRef partition = x.getPartition();
        if (partition != null) {
            for (wang.yeting.sql.ast.statement.SQLPartitionRef.Item item : partition.getItems()) {
                wang.yeting.sql.ast.expr.SQLIdentifierExpr columnName = item.getColumnName();
                columnName.setResolvedTableSource(table);
                columnName.accept(this);
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLExportTableStatement x) {
        final wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTable();
        if (table != null) {
            table.accept(this);
        }

        for (wang.yeting.sql.ast.statement.SQLAssignItem item : x.getPartition()) {
            final wang.yeting.sql.ast.SQLExpr target = item.getTarget();
            if (target instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) target).setResolvedTableSource(table);
                target.accept(this);
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLImportTableStatement x) {
        final wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTable();
        if (table != null) {
            table.accept(this);
        }

        for (wang.yeting.sql.ast.statement.SQLAssignItem item : x.getPartition()) {
            final wang.yeting.sql.ast.SQLExpr target = item.getTarget();
            if (target instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                ((wang.yeting.sql.ast.expr.SQLIdentifierExpr) target).setResolvedTableSource(table);
                target.accept(this);
            }
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLCreateOutlineStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        if (x.getOn() != null) {
            x.getOn().accept(this);
        }

        if (x.getTo() != null) {
            x.getTo().accept(this);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDumpStatement x) {
        if (repository != null
                && x.getParent() == null) {
            repository.resolve(x);
        }

        final wang.yeting.sql.ast.statement.SQLExprTableSource into = x.getInto();
        if (into != null) {
            into.accept(this);
        }

        final wang.yeting.sql.ast.statement.SQLSelect select = x.getSelect();
        if (select != null) {
            select.accept(this);
        }

        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLDropOutlineStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterOutlineStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableArchivePartition x) {
        return true;
    }

    @Override
    public boolean visit(HiveCreateTableStatement x) {
        return visit((wang.yeting.sql.ast.statement.SQLCreateTableStatement) x);
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCopyFromStatement x) {
        wang.yeting.sql.ast.statement.SQLExprTableSource table = x.getTable();
        if (table != null) {
            table.accept(this);
        }

        for (wang.yeting.sql.ast.SQLName column : x.getColumns()) {
            addColumn(table.getName(), column.getSimpleName());
        }

        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLCloneTableStatement x) {
        wang.yeting.sql.ast.statement.SQLExprTableSource from = x.getFrom();
        if (from != null) {
            TableStat stat = getTableStat(from.getName());
            if (stat != null) {
                stat.incrementSelectCount();
            }
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource to = x.getTo();
        if (to != null) {
            TableStat stat = getTableStat(to.getName());
            if (stat != null) {
                stat.incrementInsertCount();
            }
        }
        return false;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLSyncMetaStatement x) {
        return false;
    }

    public List<wang.yeting.sql.ast.SQLName> getOriginalTables() {
        return originalTables;
    }

    @Override
    public boolean visit(wang.yeting.sql.ast.statement.SQLUnique x) {
        for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem column : x.getColumns()) {
            column.accept(this);
        }
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLSavePointStatement x) {
        return false;
    }

    public boolean visit(wang.yeting.sql.ast.statement.SQLShowPartitionsStmt x) {
        setMode(x, Mode.DESC);
        return true;
    }

    protected class OrderByStatVisitor extends SQLASTVisitorAdapter {

        private final wang.yeting.sql.ast.SQLOrderBy orderBy;

        public OrderByStatVisitor(wang.yeting.sql.ast.SQLOrderBy orderBy) {
            this.orderBy = orderBy;
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : orderBy.getItems()) {
                item.getExpr().setParent(item);
            }
        }

        public wang.yeting.sql.ast.SQLOrderBy getOrderBy() {
            return orderBy;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
            return visitOrderBy(x);
        }
    }

    protected class MySqlOrderByStatVisitor extends MySqlASTVisitorAdapter {

        private final wang.yeting.sql.ast.SQLOrderBy orderBy;

        public MySqlOrderByStatVisitor(wang.yeting.sql.ast.SQLOrderBy orderBy) {
            this.orderBy = orderBy;
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : orderBy.getItems()) {
                item.getExpr().setParent(item);
            }
        }

        public wang.yeting.sql.ast.SQLOrderBy getOrderBy() {
            return orderBy;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
            return visitOrderBy(x);
        }
    }

    protected class PGOrderByStatVisitor extends PGASTVisitorAdapter {

        private final wang.yeting.sql.ast.SQLOrderBy orderBy;

        public PGOrderByStatVisitor(wang.yeting.sql.ast.SQLOrderBy orderBy) {
            this.orderBy = orderBy;
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : orderBy.getItems()) {
                item.getExpr().setParent(item);
            }
        }

        public wang.yeting.sql.ast.SQLOrderBy getOrderBy() {
            return orderBy;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
            return visitOrderBy(x);
        }
    }

    protected class OracleOrderByStatVisitor extends OracleASTVisitorAdapter {

        private final wang.yeting.sql.ast.SQLOrderBy orderBy;

        public OracleOrderByStatVisitor(wang.yeting.sql.ast.SQLOrderBy orderBy) {
            this.orderBy = orderBy;
            for (wang.yeting.sql.ast.statement.SQLSelectOrderByItem item : orderBy.getItems()) {
                item.getExpr().setParent(item);
            }
        }

        public wang.yeting.sql.ast.SQLOrderBy getOrderBy() {
            return orderBy;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIdentifierExpr x) {
            return visitOrderBy(x);
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLPropertyExpr x) {
            wang.yeting.sql.ast.SQLExpr unwrapped = unwrapExpr(x);
            if (unwrapped instanceof wang.yeting.sql.ast.expr.SQLPropertyExpr) {
                visitOrderBy((wang.yeting.sql.ast.expr.SQLPropertyExpr) unwrapped);
            } else if (unwrapped instanceof wang.yeting.sql.ast.expr.SQLIdentifierExpr) {
                visitOrderBy((wang.yeting.sql.ast.expr.SQLIdentifierExpr) unwrapped);
            }
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.expr.SQLIntegerExpr x) {
            return visitOrderBy(x);
        }
    }
}

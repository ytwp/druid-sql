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
package wang.yeting.sql.repository;

import wang.yeting.sql.DbType;
import wang.yeting.sql.FastsqlException;
import wang.yeting.sql.SQLUtils;
import wang.yeting.sql.ast.SQLDataType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatement;
import wang.yeting.sql.ast.expr.SQLAllColumnExpr;
import wang.yeting.sql.ast.expr.SQLIdentifierExpr;
import wang.yeting.sql.ast.expr.SQLIntegerExpr;
import wang.yeting.sql.ast.expr.SQLPropertyExpr;
import wang.yeting.sql.ast.statement.SQLShowColumnsStatement;
import wang.yeting.sql.ast.statement.SQLShowCreateTableStatement;
import wang.yeting.sql.dialect.hive.stmt.HiveCreateTableStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import wang.yeting.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import wang.yeting.sql.dialect.oracle.ast.stmt.OracleCreateTableStatement;
import wang.yeting.sql.dialect.oracle.visitor.OracleASTVisitorAdapter;
import wang.yeting.sql.logging.Log;
import wang.yeting.sql.logging.LogFactory;
import wang.yeting.sql.repository.function.Function;
import wang.yeting.sql.util.FnvHash;
import wang.yeting.sql.visitor.SQLASTVisitor;
import wang.yeting.sql.visitor.SQLASTVisitorAdapter;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wenshao on 03/06/2017.
 */
public class SchemaRepository {
    private static Log LOG = LogFactory.getLog(SchemaRepository.class);
    protected final Map<Long, Function> internalFunctions = new ConcurrentHashMap<Long, Function>(16, 0.75f, 1);
    protected DbType dbType;
    protected DbType schemaDbType;
    protected SQLASTVisitor consoleVisitor;
    protected Map<String, Schema> schemas = new LinkedHashMap<String, Schema>();
    protected SchemaLoader schemaLoader;
    private Schema defaultSchema;

    public SchemaRepository() {

    }

    public SchemaRepository(DbType dbType) {
        this(dbType, dbType);
    }

    public SchemaRepository(DbType dbType, DbType schemaDbType) {
        if (dbType == null) {
            dbType = DbType.other;
        }
        if (schemaDbType == null) {
            schemaDbType = dbType;
        }

        this.dbType = dbType;
        this.schemaDbType = schemaDbType;

        switch (dbType) {
            case mysql:
                consoleVisitor = new MySqlConsoleSchemaVisitor();
                break;
            case oracle:
                consoleVisitor = new OracleConsoleSchemaVisitor();
                break;
            default:
                consoleVisitor = new DefaultConsoleSchemaVisitor();
                break;
        }
    }

    public DbType getDbType() {
        return dbType;
    }

    public String getDefaultSchemaName() {
        return getDefaultSchema().getName();
    }

    public Schema findSchema(String schema) {
        return findSchema(schema, false);
    }

    protected Schema findSchema(String name, boolean create) {
        if (name == null || name.length() == 0) {
            return getDefaultSchema();
        }

        name = SQLUtils.normalize(name);
        String normalizedName = name.toLowerCase();

        if (getDefaultSchema() != null && defaultSchema.getName() == null && create) {
            defaultSchema.setName(name);
            schemas.put(normalizedName, defaultSchema);
            return defaultSchema;
        }

        Schema schema = schemas.get(normalizedName);
        if (schema == null && create) {
            int p = name.indexOf('.');

            String catalog = null;
            if (p != -1) {
                catalog = name.substring(0, p);
            }
            schema = new Schema(this, catalog, name);
            schemas.put(normalizedName, schema);
        }
        return schema;
    }

    public Schema getDefaultSchema() {
        if (defaultSchema == null) {
            defaultSchema = new Schema(this);
        }

        return defaultSchema;
    }

    public void setDefaultSchema(String name) {
        if (name == null) {
            defaultSchema = null;
            return;
        }

        String normalizedName = SQLUtils.normalize(name)
                .toLowerCase();

        Schema defaultSchema = schemas.get(normalizedName);
        if (defaultSchema != null) {
            this.defaultSchema = defaultSchema;
            return;
        }

        if (this.defaultSchema != null
                && this.defaultSchema.getName() == null) {
            this.defaultSchema.setName(name);

            schemas.put(normalizedName, this.defaultSchema);
            return;
        }

        defaultSchema = new Schema(this);
        defaultSchema.setName(name);
        schemas.put(normalizedName, defaultSchema);
        this.defaultSchema = defaultSchema;
    }

    public void setDefaultSchema(Schema schema) {
        this.defaultSchema = schema;
    }

    public SchemaObject findTable(String tableName) {
        if (tableName.indexOf('.') != -1) {
            SQLExpr expr = SQLUtils.toSQLExpr(tableName, dbType);
            if (!(expr instanceof SQLIdentifierExpr)) {
                return findTable((SQLName) expr);
            }
        }
        SchemaObject object = getDefaultSchema()
                .findTable(tableName);

        if (object != null) {
            return object;
        }

        String ddl = loadDDL(tableName);
        if (ddl == null) {
            return null;
        }

        DbType schemaDbType = this.schemaDbType;
        if (schemaDbType == null) {
            schemaDbType = dbType;
        }

        SchemaObject schemaObject = acceptDDL(ddl, schemaDbType);
        if (schemaObject != null) {
            return schemaObject;
        }

        return getDefaultSchema()
                .findTable(tableName);
    }

    public SchemaObject findView(String viewName) {
        SchemaObject object = getDefaultSchema()
                .findView(viewName);

        if (object != null) {
            return object;
        }

        String ddl = loadDDL(viewName);
        if (ddl == null) {
            return null;
        }

        acceptDDL(ddl);

        return getDefaultSchema()
                .findView(viewName);
    }

    public SchemaObject findTable(long tableNameHash) {
        return getDefaultSchema()
                .findTable(tableNameHash);
    }

    public SchemaObject findTableOrView(String tableName) {
        return findTableOrView(tableName, true);
    }

    public SchemaObject findTableOrView(String tableName, boolean onlyCurrent) {
        Schema schema = getDefaultSchema();

        SchemaObject object = schema.findTableOrView(tableName);
        if (object != null) {
            return object;
        }

        for (Schema s : this.schemas.values()) {
            if (s == schema) {
                continue;
            }

            object = schema.findTableOrView(tableName);
            if (object != null) {
                return object;
            }
        }

        String ddl = loadDDL(tableName);
        if (ddl == null) {
            return null;
        }

        acceptDDL(ddl);

        // double check
        object = schema.findTableOrView(tableName);
        if (object != null) {
            return object;
        }

        for (Schema s : this.schemas.values()) {
            if (s == schema) {
                continue;
            }

            object = schema.findTableOrView(tableName);
            if (object != null) {
                return object;
            }
        }

        return null;
    }

    public Collection<Schema> getSchemas() {
        return schemas.values();
    }

    public SchemaObject findFunction(String functionName) {
        return getDefaultSchema().findFunction(functionName);
    }

    public void acceptDDL(String ddl) {
        acceptDDL(ddl, schemaDbType);
    }

    public SchemaObject acceptDDL(String ddl, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(ddl, dbType);
        for (SQLStatement stmt : stmtList) {
            if (stmt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                SchemaObject schemaObject = acceptCreateTable((wang.yeting.sql.ast.statement.SQLCreateTableStatement) stmt);
                if (stmtList.size() == 1) {
                    return schemaObject;
                }
            } else if (stmt instanceof wang.yeting.sql.ast.statement.SQLCreateViewStatement) {
                SchemaObject schemaObject = acceptView((wang.yeting.sql.ast.statement.SQLCreateViewStatement) stmt);
                if (stmtList.size() == 1) {
                    return schemaObject;
                }
            } else {
                accept(stmt);
            }
        }

        return null;
    }

    public void accept(SQLStatement stmt) {
        stmt.accept(consoleVisitor);
    }

    public boolean isSequence(String name) {
        return getDefaultSchema().isSequence(name);
    }

    public SchemaObject findTable(wang.yeting.sql.ast.statement.SQLTableSource tableSource, String alias) {
        return getDefaultSchema().findTable(tableSource, alias);
    }

    public wang.yeting.sql.ast.statement.SQLColumnDefinition findColumn(wang.yeting.sql.ast.statement.SQLTableSource tableSource, wang.yeting.sql.ast.statement.SQLSelectItem selectItem) {
        return getDefaultSchema().findColumn(tableSource, selectItem);
    }

    public wang.yeting.sql.ast.statement.SQLColumnDefinition findColumn(wang.yeting.sql.ast.statement.SQLTableSource tableSource, SQLExpr expr) {
        return getDefaultSchema().findColumn(tableSource, expr);
    }

    public SchemaObject findTable(wang.yeting.sql.ast.statement.SQLTableSource tableSource, wang.yeting.sql.ast.statement.SQLSelectItem selectItem) {
        return getDefaultSchema().findTable(tableSource, selectItem);
    }

    public SchemaObject findTable(wang.yeting.sql.ast.statement.SQLTableSource tableSource, SQLExpr expr) {
        return getDefaultSchema().findTable(tableSource, expr);
    }

    public Map<String, SchemaObject> getTables(wang.yeting.sql.ast.statement.SQLTableSource x) {
        return getDefaultSchema().getTables(x);
    }

    public boolean removeTable(SQLName name) {
        return getDefaultSchema()
                .removeObject(
                        name.nameHashCode64());
    }

    public int getTableCount() {
        return getDefaultSchema().getTableCount();
    }

    public Collection<SchemaObject> getObjects() {
        return getDefaultSchema().getObjects();
    }

    public int getViewCount() {
        return getDefaultSchema().getViewCount();
    }

    public void resolve(wang.yeting.sql.ast.statement.SQLSelectStatement stmt, SchemaResolveVisitor.Option... options) {
        if (stmt == null) {
            return;
        }

        SchemaResolveVisitor resolveVisitor = createResolveVisitor(options);
        resolveVisitor.visit(stmt);
    }

    public void resolve(wang.yeting.sql.ast.statement.SQLSelect select, SchemaResolveVisitor.Option... options) {
        if (select == null) {
            return;
        }

        SchemaResolveVisitor resolveVisitor = createResolveVisitor(options);
        resolveVisitor.visit(select);
    }

    public void resolve(wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock, SchemaResolveVisitor.Option... options) {
        if (queryBlock == null) {
            return;
        }

        SchemaResolveVisitor resolveVisitor = createResolveVisitor(options);
        resolveVisitor.visit(queryBlock);
    }

    public void resolve(SQLStatement stmt, SchemaResolveVisitor.Option... options) {
        if (stmt == null) {
            return;
        }

        SchemaResolveVisitor resolveVisitor = createResolveVisitor(options);
        if (stmt instanceof wang.yeting.sql.ast.statement.SQLSelectStatement) {
            resolveVisitor.visit((wang.yeting.sql.ast.statement.SQLSelectStatement) stmt);
        } else {
            stmt.accept(resolveVisitor);
        }
    }

    private SchemaResolveVisitor createResolveVisitor(SchemaResolveVisitor.Option... options) {
        int optionsValue = SchemaResolveVisitor.Option.of(options);

        SchemaResolveVisitor resolveVisitor;
        switch (dbType) {
            case mysql:
            case mariadb:
            case sqlite:
                resolveVisitor = new SchemaResolveVisitorFactory.MySqlResolveVisitor(this, optionsValue);
                break;
            case oracle:
                resolveVisitor = new SchemaResolveVisitorFactory.OracleResolveVisitor(this, optionsValue);
                break;
            case db2:
                resolveVisitor = new SchemaResolveVisitorFactory.DB2ResolveVisitor(this, optionsValue);
                break;
            case odps:
                resolveVisitor = new SchemaResolveVisitorFactory.OdpsResolveVisitor(this, optionsValue);
                break;
            case hive:
                resolveVisitor = new SchemaResolveVisitorFactory.HiveResolveVisitor(this, optionsValue);
                break;
            case postgresql:
            case edb:
                resolveVisitor = new SchemaResolveVisitorFactory.PGResolveVisitor(this, optionsValue);
                break;
            case sqlserver:
                resolveVisitor = new SchemaResolveVisitorFactory.SQLServerResolveVisitor(this, optionsValue);
                break;
            default:
                resolveVisitor = new SchemaResolveVisitorFactory.SQLResolveVisitor(this, optionsValue);
                break;
        }

        return resolveVisitor;
    }

    public String resolve(String input) {
        SchemaResolveVisitor visitor
                = createResolveVisitor(
                SchemaResolveVisitor.Option.ResolveAllColumn,
                SchemaResolveVisitor.Option.ResolveIdentifierAlias);

        List<SQLStatement> stmtList = SQLUtils.parseStatements(input, dbType);

        for (SQLStatement stmt : stmtList) {
            stmt.accept(visitor);
        }

        return SQLUtils.toSQLString(stmtList, dbType);
    }

    public String console(String input) {
        try {
            StringBuffer buf = new StringBuffer();

            List<SQLStatement> stmtList = SQLUtils.parseStatements(input, dbType);

            for (SQLStatement stmt : stmtList) {
                if (stmt instanceof SQLShowColumnsStatement) {
                    SQLShowColumnsStatement showColumns = ((SQLShowColumnsStatement) stmt);
                    SQLName db = showColumns.getDatabase();
                    Schema schema;
                    if (db == null) {
                        schema = getDefaultSchema();
                    } else {
                        schema = findSchema(db.getSimpleName());
                    }

                    SQLName table = null;
                    SchemaObject schemaObject = null;
                    if (schema != null) {
                        table = showColumns.getTable();
                        schemaObject = schema.findTable(table.nameHashCode64());
                    }

                    if (schemaObject == null) {
                        buf.append("ERROR 1146 (42S02): Table '" + table + "' doesn't exist\n");
                    } else {
                        MySqlCreateTableStatement createTableStmt = (MySqlCreateTableStatement) schemaObject.getStatement();
                        createTableStmt.showCoumns(buf);
                    }
                } else if (stmt instanceof SQLShowCreateTableStatement) {
                    SQLShowCreateTableStatement showCreateTableStmt = (SQLShowCreateTableStatement) stmt;
                    SQLName table = showCreateTableStmt.getName();
                    SchemaObject schemaObject = findTable(table);
                    if (schemaObject == null) {
                        buf.append("ERROR 1146 (42S02): Table '" + table + "' doesn't exist\n");
                    } else {
                        MySqlCreateTableStatement createTableStmt = (MySqlCreateTableStatement) schemaObject.getStatement();
                        createTableStmt.output(buf);
                    }
                } else if (stmt instanceof MySqlRenameTableStatement) {
                    MySqlRenameTableStatement renameStmt = (MySqlRenameTableStatement) stmt;
                    for (MySqlRenameTableStatement.Item item : renameStmt.getItems()) {
                        renameTable(item.getName(), item.getTo());
                    }
                } else if (stmt instanceof wang.yeting.sql.ast.statement.SQLShowTablesStatement) {
                    wang.yeting.sql.ast.statement.SQLShowTablesStatement showTables = (wang.yeting.sql.ast.statement.SQLShowTablesStatement) stmt;
                    SQLName database = showTables.getDatabase();

                    Schema schema;
                    if (database == null) {
                        schema = getDefaultSchema();
                    } else {
                        schema = findSchema(database.getSimpleName());
                    }
                    if (schema != null) {
                        for (String table : schema.showTables()) {
                            buf.append(table);
                            buf.append('\n');
                        }
                    }
                } else {
                    stmt.accept(consoleVisitor);
                }
            }

            if (buf.length() == 0) {
                return "\n";
            }

            return buf.toString();
        } catch (IOException ex) {
            throw new FastsqlException("exeucte command error.", ex);
        }
    }

    public SchemaObject findTable(SQLName name) {
        if (name instanceof SQLIdentifierExpr) {
            return findTable(
                    ((SQLIdentifierExpr) name).getName());
        }

        if (name instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) name;
            SQLExpr owner = propertyExpr.getOwner();
            String schema;
            String catalog = null;
            if (owner instanceof SQLIdentifierExpr) {
                schema = ((SQLIdentifierExpr) owner).getName();
            } else if (owner instanceof SQLPropertyExpr) {
                schema = ((SQLPropertyExpr) owner).getName();
                catalog = ((SQLPropertyExpr) owner).getOwnernName();
            } else {
                return null;
            }

            long tableHashCode64 = propertyExpr.nameHashCode64();

            Schema schemaObj = findSchema(schema, false);
            if (schemaObj != null) {
                SchemaObject table = schemaObj.findTable(tableHashCode64);
                if (table != null) {
                    return table;
                }
            }

            String ddl = loadDDL(catalog, schema, propertyExpr.getName());

            if (ddl == null) {
                schemaObj = findSchema(schema, true);
            } else {
                List<SQLStatement> stmtList = SQLUtils.parseStatements(ddl, schemaDbType);
                for (SQLStatement stmt : stmtList) {
                    accept(stmt);
                }

                if (stmtList.size() == 1) {
                    SQLStatement stmt = stmtList.get(0);
                    if (stmt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                        wang.yeting.sql.ast.statement.SQLCreateTableStatement createStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) stmt;
                        String schemaName = createStmt.getSchema();
                        schemaObj = findSchema(schemaName, true);
                    }
                }
            }

            if (schemaObj == null) {
                return null;
            }

            return schemaObj.findTable(tableHashCode64);
        }

        return null;
    }

    public SchemaObject findView(SQLName name) {
        if (name instanceof SQLIdentifierExpr) {
            return findView(
                    ((SQLIdentifierExpr) name).getName());
        }

        if (name instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) name;
            SQLExpr owner = propertyExpr.getOwner();
            String schema;
            String catalog = null;
            if (owner instanceof SQLIdentifierExpr) {
                schema = ((SQLIdentifierExpr) owner).getName();
            } else if (owner instanceof SQLPropertyExpr) {
                schema = ((SQLPropertyExpr) owner).getName();
                catalog = ((SQLPropertyExpr) owner).getOwnernName();
            } else {
                return null;
            }

            long tableHashCode64 = propertyExpr.nameHashCode64();

            Schema schemaObj = findSchema(schema, false);
            if (schemaObj != null) {
                SchemaObject table = schemaObj.findView(tableHashCode64);
                if (table != null) {
                    return table;
                }
            }

            String ddl = loadDDL(catalog, schema, propertyExpr.getName());

            if (ddl == null) {
                schemaObj = findSchema(schema, true);
            } else {
                List<SQLStatement> stmtList = SQLUtils.parseStatements(ddl, schemaDbType);
                for (SQLStatement stmt : stmtList) {
                    accept(stmt);
                }

                if (stmtList.size() == 1) {
                    SQLStatement stmt = stmtList.get(0);
                    if (stmt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                        wang.yeting.sql.ast.statement.SQLCreateTableStatement createStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) stmt;
                        String schemaName = createStmt.getSchema();
                        schemaObj = findSchema(schemaName, true);
                    }
                }
            }

            if (schemaObj == null) {
                return null;
            }

            return schemaObj.findView(tableHashCode64);
        }

        return null;
    }

    private boolean renameTable(SQLName name, SQLName to) {
        Schema schema;
        if (name instanceof SQLPropertyExpr) {
            String schemaName = ((SQLPropertyExpr) name).getOwnernName();
            schema = findSchema(schemaName);
        } else {
            schema = getDefaultSchema();
        }

        if (schema == null) {
            return false;
        }

        long nameHashCode64 = name.nameHashCode64();
        SchemaObject schemaObject = schema.findTable(nameHashCode64);
        if (schemaObject != null) {
            MySqlCreateTableStatement createTableStmt = (MySqlCreateTableStatement) schemaObject.getStatement();
            if (createTableStmt != null) {
                createTableStmt.setName(to.clone());
                acceptCreateTable(createTableStmt);
            }

            schema.objects.remove(nameHashCode64);
        }
        return true;
    }


    public SchemaObject findTable(wang.yeting.sql.ast.statement.SQLExprTableSource x) {
        if (x == null) {
            return null;
        }

        SQLExpr expr = x.getExpr();
        if (expr instanceof SQLName) {
            return findTable((SQLName) expr);
        }

        return null;
    }

    SchemaObject acceptCreateTable(MySqlCreateTableStatement x) {
        wang.yeting.sql.ast.statement.SQLExprTableSource like = x.getLike();
        if (like != null) {
            SchemaObject table = findTable((SQLName) like.getExpr());
            if (table != null) {
                MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) table.getStatement();
                MySqlCreateTableStatement stmtCloned = stmt.clone();
                stmtCloned.setName(x.getName().clone());
                acceptCreateTable((wang.yeting.sql.ast.statement.SQLCreateTableStatement) stmtCloned);
                return table;
            }
        }

        return acceptCreateTable((wang.yeting.sql.ast.statement.SQLCreateTableStatement) x);
    }

    SchemaObject acceptCreateTable(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
        wang.yeting.sql.ast.statement.SQLCreateTableStatement x1 = x.clone();
        String schemaName = x1.getSchema();

        Schema schema = findSchema(schemaName, true);

        wang.yeting.sql.ast.statement.SQLSelect select = x1.getSelect();

        if (select != null) {
            select.accept(createResolveVisitor(SchemaResolveVisitor.Option.ResolveAllColumn));

            wang.yeting.sql.ast.statement.SQLSelectQueryBlock queryBlock = select.getFirstQueryBlock();
            this.resolve(queryBlock);

            if (queryBlock != null) {
                List<wang.yeting.sql.ast.statement.SQLSelectItem> selectList = queryBlock.getSelectList();
                for (wang.yeting.sql.ast.statement.SQLSelectItem selectItem : selectList) {
                    SQLExpr selectItemExpr = selectItem.getExpr();
                    if (selectItemExpr instanceof SQLAllColumnExpr
                            || (selectItemExpr instanceof SQLPropertyExpr && ((SQLPropertyExpr) selectItemExpr).getName().equals("*"))) {
                        continue;
                    }

                    wang.yeting.sql.ast.statement.SQLColumnDefinition column = null;
                    if (selectItemExpr instanceof SQLName) {
                        final wang.yeting.sql.ast.statement.SQLColumnDefinition resolvedColumn = ((SQLName) selectItemExpr).getResolvedColumn();
                        if (resolvedColumn != null) {
                            column = new wang.yeting.sql.ast.statement.SQLColumnDefinition();
                            column.setDataType(
                                    selectItem.computeDataType());
                            if (DbType.mysql == dbType) {
                                if (resolvedColumn.getDefaultExpr() != null) {
                                    column.setDefaultExpr(resolvedColumn.getDefaultExpr().clone());
                                }
                                if (resolvedColumn.getConstraints().size() > 0) {
                                    for (wang.yeting.sql.ast.statement.SQLColumnConstraint constraint : resolvedColumn.getConstraints()) {
                                        column.addConstraint(constraint.clone());
                                    }
                                }
                                if (resolvedColumn.getComment() != null) {
                                    column.setComment(resolvedColumn.getComment());
                                }
                            }
                        }
                    }

                    if (column == null) {
                        column = new wang.yeting.sql.ast.statement.SQLColumnDefinition();
                        column.setDataType(
                                selectItem.computeDataType());
                    }

                    String name = selectItem.computeAlias();
                    column.setName(name);
                    column.setDbType(dbType);
                    x1.addColumn(column);
                }
                if (x1.getTableElementList().size() > 0) {
                    x1.setSelect(null);
                }
            }
        }

        wang.yeting.sql.ast.statement.SQLExprTableSource like = x1.getLike();
        if (like != null) {
            SchemaObject tableObject = null;

            SQLName name = like.getName();
            if (name != null) {
                tableObject = findTable(name);
            }

            wang.yeting.sql.ast.statement.SQLCreateTableStatement tableStmt = null;
            if (tableObject != null) {
                SQLStatement stmt = tableObject.getStatement();
                if (stmt instanceof wang.yeting.sql.ast.statement.SQLCreateTableStatement) {
                    tableStmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) stmt;
                }
            }

            if (tableStmt != null) {
                SQLName tableName = x1.getName();
                tableStmt.cloneTo(x1);
                x1.setName(tableName);
                x1.setLike((wang.yeting.sql.ast.statement.SQLExprTableSource) null);
            }
        }

        x1.setSchema(null);

        String name = x1.computeName();
        SchemaObject table = schema.findTableOrView(name);
        if (table != null) {
            if (x1.isIfNotExists()) {
                return table;
            }

            LOG.info("replaced table '" + name + "'");
        }

        table = new SchemaObject(schema, name, SchemaObjectType.Table, x1);
        schema.objects.put(table.nameHashCode64(), table);
        return table;
    }

    boolean acceptDropTable(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
        for (wang.yeting.sql.ast.statement.SQLExprTableSource table : x.getTableSources()) {
            String schemaName = table.getSchema();
            Schema schema = findSchema(schemaName, false);
            if (schema == null) {
                continue;
            }
            long nameHashCode64 = table.getName().nameHashCode64();
            schema.objects.remove(nameHashCode64);
        }
        return true;
    }

    SchemaObject acceptView(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
        String schemaName = x.getSchema();

        Schema schema = findSchema(schemaName, true);

        String name = x.computeName();
        SchemaObject view = schema.findTableOrView(name);
        if (view != null) {
            return view;
        }

        SchemaObject object = new SchemaObject(schema, name, SchemaObjectType.View, x.clone());
        long nameHashCode64 = FnvHash.hashCode64(name);
        schema.objects.put(nameHashCode64, object);
        return object;
    }

    boolean acceptView(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {
        String schemaName = x.getSchema();

        Schema schema = findSchema(schemaName, true);

        String name = x.computeName();
        SchemaObject view = schema.findTableOrView(name);
        if (view != null) {
            return false;
        }

        SchemaObject object = new SchemaObject(schema, name, SchemaObjectType.View, x.clone());
        schema.objects.put(object.nameHashCode64(), object);
        return true;
    }

    boolean acceptDropIndex(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
        if (x.getTableName() == null) {
            return false;
        }
        SQLName table = x.getTableName().getName();
        SchemaObject object = findTable(table);

        if (object != null) {
            wang.yeting.sql.ast.statement.SQLCreateTableStatement stmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) object.getStatement();
            if (stmt != null) {
                stmt.apply(x);
                return true;
            }
        }

        return false;
    }

    boolean acceptCreateIndex(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {
        String schemaName = x.getSchema();

        Schema schema = findSchema(schemaName, true);

        String name = x.getName().getSimpleName();
        SchemaObject object = new SchemaObject(schema, name, SchemaObjectType.Index, x.clone());
        schema.objects.put(object.nameHashCode64(), object);

        return true;
    }

    boolean acceptCreateFunction(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
        String schemaName = x.getSchema();
        Schema schema = findSchema(schemaName, true);

        String name = x.getName().getSimpleName();
        SchemaObject object = new SchemaObject(schema, name, SchemaObjectType.Function, x.clone());
        schema.functions.put(object.nameHashCode64(), object);

        return true;
    }

    boolean acceptAlterTable(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
        String schemaName = x.getSchema();
        Schema schema = findSchema(schemaName, true);

        SchemaObject object = schema.findTable(x.nameHashCode64());
        if (object != null) {
            wang.yeting.sql.ast.statement.SQLCreateTableStatement stmt = (wang.yeting.sql.ast.statement.SQLCreateTableStatement) object.getStatement();
            if (stmt != null) {
                stmt.apply(x);
                return true;
            }
        }

        return false;
    }

    public boolean acceptCreateSequence(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {
        String schemaName = x.getSchema();
        Schema schema = findSchema(schemaName, true);

        String name = x.getName().getSimpleName();
        SchemaObject object = new SchemaObject(schema, name, SchemaObjectType.Sequence);
        schema.objects.put(object.nameHashCode64(), object);
        return false;
    }

    public boolean acceptDropSequence(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {
        String schemaName = x.getSchema();
        Schema schema = findSchema(schemaName, true);

        long nameHashCode64 = x.getName().nameHashCode64();
        schema.objects.remove(nameHashCode64);
        return false;
    }

    public SQLDataType findFuntionReturnType(long functionNameHashCode) {
        if (functionNameHashCode == FnvHash.Constants.LEN || functionNameHashCode == FnvHash.Constants.LENGTH) {
            return SQLIntegerExpr.DATA_TYPE;
        }

        return null;
    }

    protected String loadDDL(String table) {
        if (table == null) {
            return null;
        }

        table = SQLUtils.normalize(table, schemaDbType);

        if (schemaLoader != null) {
            return schemaLoader.loadDDL(null, null, table);
        }
        return null;
    }

    protected String loadDDL(String schema, String table) {
        if (table == null) {
            return null;
        }

        table = SQLUtils.normalize(table, dbType);
        if (schema != null) {
            schema = SQLUtils.normalize(schema, dbType);
        }

        if (schemaLoader != null) {
            return schemaLoader.loadDDL(null, schema, table);
        }
        return null;
    }

    protected String loadDDL(String catalog, String schema, String table) {
        if (table == null) {
            return null;
        }

        table = SQLUtils.normalize(table, dbType);
        if (schema != null) {
            schema = SQLUtils.normalize(schema, dbType);
        }
        if (catalog != null) {
            catalog = SQLUtils.normalize(catalog, dbType);
        }

        if (schemaLoader != null) {
            return schemaLoader.loadDDL(catalog, schema, table);
        }
        return null;
    }

    public SchemaLoader getSchemaLoader() {
        return schemaLoader;
    }

    public void setSchemaLoader(SchemaLoader schemaLoader) {
        this.schemaLoader = schemaLoader;
    }

    public static interface SchemaLoader {
        String loadDDL(String catalog, String schema, String objectName);
    }

    public class MySqlConsoleSchemaVisitor extends MySqlASTVisitorAdapter {
        @Override
        public boolean visit(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {
            acceptDropSequence(x);
            return false;
        }

        @Override
        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {
            acceptCreateSequence(x);
            return false;
        }

        public boolean visit(HiveCreateTableStatement x) {
            acceptCreateTable(x);
            return false;
        }

        @Override
        public boolean visit(MySqlCreateTableStatement x) {
            acceptCreateTable(x);
            return false;
        }

        @Override
        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
            acceptCreateTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
            acceptDropTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
            acceptView(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {
            acceptView(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {
            acceptCreateIndex(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
            acceptCreateFunction(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
            acceptAlterTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLUseStatement x) {
            String schema = x.getDatabase().getSimpleName();
            setDefaultSchema(schema);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
            acceptDropIndex(x);
            return false;
        }
    }

    public class OracleConsoleSchemaVisitor extends OracleASTVisitorAdapter {
        public boolean visit(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {
            acceptDropSequence(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {
            acceptCreateSequence(x);
            return false;
        }

        public boolean visit(OracleCreateTableStatement x) {
            visit((wang.yeting.sql.ast.statement.SQLCreateTableStatement) x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
            acceptCreateTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
            acceptDropTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
            acceptView(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {
            acceptView(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {
            acceptCreateIndex(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
            acceptCreateFunction(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
            acceptAlterTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLUseStatement x) {
            String schema = x.getDatabase().getSimpleName();
            setDefaultSchema(schema);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
            acceptDropIndex(x);
            return false;
        }
    }

    public class DefaultConsoleSchemaVisitor extends SQLASTVisitorAdapter {
        public boolean visit(wang.yeting.sql.ast.statement.SQLDropSequenceStatement x) {
            acceptDropSequence(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateSequenceStatement x) {
            acceptCreateSequence(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateTableStatement x) {
            acceptCreateTable(x);
            return false;
        }

        public boolean visit(HiveCreateTableStatement x) {
            acceptCreateTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLDropTableStatement x) {
            acceptDropTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateViewStatement x) {
            acceptView(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLAlterViewStatement x) {
            acceptView(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateIndexStatement x) {
            acceptCreateIndex(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLCreateFunctionStatement x) {
            acceptCreateFunction(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLAlterTableStatement x) {
            acceptAlterTable(x);
            return false;
        }

        public boolean visit(wang.yeting.sql.ast.statement.SQLDropIndexStatement x) {
            acceptDropIndex(x);
            return false;
        }
    }
}

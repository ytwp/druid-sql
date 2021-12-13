package wang.yeting.sql.dialect.mysql.ast.statement;

import wang.yeting.sql.ast.statement.SQLExprTableSource;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class MySqlCheckTableStatement extends MySqlStatementImpl {
    private final List<SQLExprTableSource> tables = new ArrayList<SQLExprTableSource>();


    public MySqlCheckTableStatement() {

    }

    public void addTable(SQLExprTableSource table) {
        if (table == null) {
            return;
        }

        table.setParent(this);
        tables.add(table);
    }

    public List<SQLExprTableSource> getTables() {
        return tables;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tables);
        }
        visitor.endVisit(this);
    }
}

package wang.yeting.sql.dialect.ads.visitor;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.statement.SQLAlterTableAddColumn;
import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLCreateTableStatement;
import wang.yeting.sql.ast.statement.SQLShowColumnsStatement;
import wang.yeting.sql.dialect.mysql.ast.MySqlPrimaryKey;
import wang.yeting.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import wang.yeting.sql.visitor.SQLASTOutputVisitor;

import java.util.List;

public class AdsOutputVisitor extends SQLASTOutputVisitor implements AdsVisitor {
    public AdsOutputVisitor(Appendable appender) {
        super(appender);
    }

    public AdsOutputVisitor(Appendable appender, DbType dbType) {
        super(appender, dbType);
    }

    public AdsOutputVisitor(Appendable appender, boolean parameterized) {
        super(appender, parameterized);
    }

    public boolean visit(SQLCreateTableStatement x) {
        printCreateTable(x, true);

        List<SQLAssignItem> options = x.getTableOptions();
        if (options.size() > 0) {
            println();
            print0(ucase ? "OPTIONS (" : "options (");
            printAndAccept(options, ", ");
            print(')');
        }

        return false;
    }

    @Override
    public boolean visit(SQLAlterTableAddColumn x) {
        print0(ucase ? "ADD COLUMN " : "add column ");
        printAndAccept(x.getColumns(), ", ");
        return false;
    }

    @Override
    public boolean visit(SQLShowColumnsStatement x) {
        print0(ucase ? "SHOW COLUMNS" : "show columns");

        if (x.getTable() != null) {
            print0(ucase ? " IN " : " in ");
            x.getTable().accept(this);
        }

        return false;
    }


    @Override
    public void endVisit(MySqlPrimaryKey x) {

    }

    @Override
    public void endVisit(MySqlCreateTableStatement x) {

    }
}

package wang.yeting.sql.dialect.blink.ast;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.statement.SQLCreateTableStatement;

public class BlinkCreateTableStatement extends SQLCreateTableStatement {
    private SQLExpr periodFor;

    public BlinkCreateTableStatement() {
        dbType = DbType.blink;
    }

    public SQLExpr getPeriodFor() {
        return periodFor;
    }

    public void setPeriodFor(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.periodFor = x;
    }
}

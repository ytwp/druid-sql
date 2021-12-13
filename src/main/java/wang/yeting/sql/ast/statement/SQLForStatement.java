package wang.yeting.sql.ast.statement;

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.SQLStatement;
import wang.yeting.sql.ast.SQLStatementImpl;
import wang.yeting.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLForStatement extends SQLStatementImpl {
    protected SQLName index;
    protected SQLExpr range;

    protected List<SQLStatement> statements = new ArrayList<SQLStatement>();

    public SQLForStatement() {

    }

    public SQLName getIndex() {
        return index;
    }

    public void setIndex(SQLName index) {
        this.index = index;
    }

    public SQLExpr getRange() {
        return range;
    }

    public void setRange(SQLExpr range) {
        if (range != null) {
            range.setParent(this);
        }
        this.range = range;
    }

    public List<SQLStatement> getStatements() {
        return statements;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, index);
            acceptChild(v, range);
            acceptChild(v, statements);
        }
        v.endVisit(this);

    }
}

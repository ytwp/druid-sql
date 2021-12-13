package wang.yeting.sql.dialect.mysql.ast.statement;

import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.statement.SQLDropStatement;
import wang.yeting.sql.dialect.mysql.ast.FullTextType;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class MysqlDropFullTextStatement extends MySqlStatementImpl implements SQLDropStatement {

    private FullTextType type;

    private SQLName name;

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public FullTextType getType() {
        return type;
    }

    public void setType(FullTextType type) {
        this.type = type;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

}

package wang.yeting.sql.dialect.mysql.ast.statement;

import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.statement.SQLShowStatement;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * version 1.0
 * Author zzy
 * Date 2019/10/8 20:06
 */
public class DrdsShowGlobalIndex extends MySqlStatementImpl implements SQLShowStatement {

    private SQLName tableName = null;

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public SQLName getTableName() {
        return tableName;
    }

    public void setTableName(SQLName tableName) {
        tableName.setParent(this);
        this.tableName = tableName;
    }

}

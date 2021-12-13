package wang.yeting.sql.dialect.mysql.ast.statement;

import wang.yeting.sql.ast.SQLName;
import wang.yeting.sql.ast.statement.SQLShowStatement;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * version 1.0
 * Author zzy
 * Date 2019/10/8 20:08
 */
public class DrdsShowMetadataLock extends MySqlStatementImpl implements SQLShowStatement {

    private SQLName schemaName = null;

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public SQLName getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(SQLName schemaName) {
        schemaName.setParent(this);
        this.schemaName = schemaName;
    }

}

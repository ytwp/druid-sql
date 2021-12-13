package wang.yeting.sql.dialect.mysql.ast.statement;

import wang.yeting.sql.ast.statement.SQLAlterTableItem;
import wang.yeting.sql.dialect.mysql.ast.MySqlObjectImpl;
import wang.yeting.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * version 1.0
 * Author zzy
 * Date 2019-06-03 15:43
 */
public class MySqlAlterTableForce extends MySqlObjectImpl implements SQLAlterTableItem {
    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}

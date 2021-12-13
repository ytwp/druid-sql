package wang.yeting.sql.ast;

import wang.yeting.sql.ast.statement.SQLSelectOrderByItem;

import java.util.List;

public interface SQLIndex extends SQLObject {
    List<SQLName> getCovering();

    List<SQLSelectOrderByItem> getColumns();
}

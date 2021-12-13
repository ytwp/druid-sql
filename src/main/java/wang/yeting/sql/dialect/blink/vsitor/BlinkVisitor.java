package wang.yeting.sql.dialect.blink.vsitor;

import wang.yeting.sql.dialect.blink.ast.BlinkCreateTableStatement;
import wang.yeting.sql.visitor.SQLASTVisitor;

public interface BlinkVisitor extends SQLASTVisitor {
    boolean visit(BlinkCreateTableStatement x);

    void endVisit(BlinkCreateTableStatement x);
}

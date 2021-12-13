package wang.yeting.sql.dialect.clickhouse.parser;

import wang.yeting.sql.ast.SQLExpr;
import wang.yeting.sql.ast.SQLOrderBy;
import wang.yeting.sql.ast.statement.SQLAssignItem;
import wang.yeting.sql.ast.statement.SQLCreateTableStatement;
import wang.yeting.sql.dialect.clickhouse.ast.ClickhouseCreateTableStatement;
import wang.yeting.sql.parser.SQLCreateTableParser;
import wang.yeting.sql.parser.SQLExprParser;
import wang.yeting.sql.parser.Token;
import wang.yeting.sql.util.FnvHash;

public class ClickhouseCreateTableParser extends SQLCreateTableParser {
    public ClickhouseCreateTableParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    protected SQLCreateTableStatement newCreateStatement() {
        return new ClickhouseCreateTableStatement();
    }

    protected void parseCreateTableRest(SQLCreateTableStatement stmt) {
        ClickhouseCreateTableStatement ckStmt = (ClickhouseCreateTableStatement) stmt;
        if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.setEngine(
                    this.exprParser.expr()
            );
        }

        if (lexer.identifierEquals("PARTITION")) {
            lexer.nextToken();
            accept(Token.BY);
            SQLExpr expr = this.exprParser.expr();
            ckStmt.setPartitionBy(expr);
        }

        if (lexer.token() == Token.ORDER) {
            SQLOrderBy orderBy = this.exprParser.parseOrderBy();
            ckStmt.setOrderBy(orderBy);
        }

        if (lexer.identifierEquals("SAMPLE")) {
            lexer.nextToken();
            accept(Token.BY);
            SQLExpr expr = this.exprParser.expr();
            ckStmt.setSampleBy(expr);
        }

        if (lexer.identifierEquals("SETTINGS")) {
            lexer.nextToken();
            for (; ; ) {
                SQLAssignItem item = this.exprParser.parseAssignItem();
                item.setParent(ckStmt);
                ckStmt.getSettings().add(item);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }

                break;
            }
        }
    }
}

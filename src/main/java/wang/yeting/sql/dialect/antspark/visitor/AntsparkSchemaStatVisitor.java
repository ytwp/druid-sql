/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2018 All Rights Reserved.
 */
package wang.yeting.sql.dialect.antspark.visitor;

import wang.yeting.sql.DbType;
import wang.yeting.sql.ast.statement.SQLCreateTableStatement;
import wang.yeting.sql.dialect.antspark.ast.AntsparkCreateTableStatement;
import wang.yeting.sql.repository.SchemaRepository;
import wang.yeting.sql.visitor.SchemaStatVisitor;

/**
 * @author peiheng.qph
 * @version $Id: AntsparkSchemaStatVisitor.java, v 0.1 2018年09月16日 23:09 peiheng.qph Exp $
 */
public class AntsparkSchemaStatVisitor extends SchemaStatVisitor implements AntsparkVisitor {
    {
        dbType = DbType.antspark;
    }

    public AntsparkSchemaStatVisitor() {
        super(DbType.antspark);
    }

    public AntsparkSchemaStatVisitor(SchemaRepository repository) {
        super(repository);
    }

    @Override
    public boolean visit(AntsparkCreateTableStatement x) {
        return super.visit((SQLCreateTableStatement) x);
    }

    @Override
    public void endVisit(AntsparkCreateTableStatement x) {
        super.endVisit((SQLCreateTableStatement) x);

    }
}
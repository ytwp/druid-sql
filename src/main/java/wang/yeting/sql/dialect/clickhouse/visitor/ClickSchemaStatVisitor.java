package wang.yeting.sql.dialect.clickhouse.visitor;

import wang.yeting.sql.DbType;
import wang.yeting.sql.repository.SchemaRepository;
import wang.yeting.sql.visitor.SchemaStatVisitor;

public class ClickSchemaStatVisitor extends SchemaStatVisitor implements ClickhouseVisitor {
    {
        dbType = DbType.antspark;
    }

    public ClickSchemaStatVisitor() {
        super(DbType.antspark);
    }

    public ClickSchemaStatVisitor(SchemaRepository repository) {
        super(repository);
    }
}

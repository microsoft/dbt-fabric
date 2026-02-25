from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
)


class TestPersistDocsFabricSpark(BasePersistDocs):
    pass


class TestPersistDocsColumnMissingFabricSpark(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsCommentOnQuotedColumnFabricSpark(BasePersistDocsCommentOnQuotedColumn):
    pass

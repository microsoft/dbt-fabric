from dbt.tests.fixtures.project import TestProjInfo


class TestProjInfoFabric(TestProjInfo):
    def get_tables_in_schema(self):
        sql = f"""
                select
                        t.name as table_name,
                        'table' as materialization
                from sys.tables t
                inner join sys.schemas s
                on s.schema_id = t.schema_id
                where lower(s.name) = '{self.test_schema.lower()}'
                union all
                select
                        v.name as table_name,
                        'view' as materialization
                from sys.views v
                inner join sys.schemas s
                on s.schema_id = v.schema_id
                where lower(s.name) = '{self.test_schema.lower()}'
                """
        result = self.run_sql(sql, fetch="all")
        return {model_name: materialization for (model_name, materialization) in result}

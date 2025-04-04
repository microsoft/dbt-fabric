from dbt.tests.fixtures.project import TestProjInfo


class TestProjInfoFabric(TestProjInfo):
    def get_tables_in_schema(self):
        sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema.tables
                where {}
                order by table_name
                """
        sql = sql.format(f"lower(table_schema) = '{self.test_schema.lower()}'")
        result = self.run_sql(sql, fetch="all")
        return {model_name: materialization for (model_name, materialization) in result}

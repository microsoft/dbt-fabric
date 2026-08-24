import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).parents[4]
MACRO_ROOT = REPOSITORY_ROOT / "dbt/include/fabric/macros/materializations"
ADAPTER_MACRO_ROOT = REPOSITORY_ROOT / "dbt/include/fabric/macros/adapters"
UTILITY_MACRO_ROOT = REPOSITORY_ROOT / "dbt/include/fabric/macros/utils"


def _read(relative_path: str) -> str:
    return (MACRO_ROOT / relative_path).read_text()


def _read_adapter_macro(relative_path: str) -> str:
    return (ADAPTER_MACRO_ROOT / relative_path).read_text()


def _read_utility_macro(relative_path: str) -> str:
    return (UTILITY_MACRO_ROOT / relative_path).read_text()


def test_materialization_macros_do_not_manage_nested_transactions():
    transaction_owned_macros = [
        "models/table/refresh.sql",
        "models/table/columns_spec_ddl.sql",
        "models/table/table.sql",
        "models/table/clone.sql",
        "functions/function.sql",
        "models/incremental/incremental.sql",
        "models/view/view.sql",
        "snapshots/snapshot.sql",
        "seeds/helpers.sql",
    ]

    for macro_path in transaction_owned_macros:
        sql = _read(macro_path).upper()
        assert "BEGIN TRANSACTION" not in sql
        assert "COMMIT TRANSACTION" not in sql
        assert "ROLLBACK TRANSACTION" not in sql


def test_table_metadata_operations_run_before_commit():
    sql = _read("models/table/table.sql")

    assert sql.index("reconcile_model_constraints") < sql.index("adapter.commit()")
    assert sql.index("create_or_update_statistics") < sql.index("adapter.commit()")


def test_table_clone_metadata_operations_run_before_commit():
    sql = _read("models/table/clone.sql")

    assert sql.index("apply_grants") < sql.index("adapter.commit()")
    assert sql.index("persist_docs") < sql.index("adapter.commit()")


def test_function_hooks_and_metadata_run_before_commit():
    sql = _read("functions/function.sql")

    assert sql.index("apply_grants") < sql.index("adapter.commit()")
    assert sql.index("persist_docs") < sql.index("adapter.commit()")
    assert sql.index("run_hooks(post_hooks, inside_transaction=True)") < sql.index(
        "adapter.commit()"
    )


def test_incremental_metadata_operations_run_before_commit():
    sql = _read("models/incremental/incremental.sql")

    assert sql.index("reconcile_model_constraints") < sql.index("adapter.commit()")
    assert sql.index("create_or_update_statistics") < sql.index("adapter.commit()")


def test_snapshot_cleanup_and_statistics_run_before_commit():
    sql = _read("snapshots/snapshot.sql")

    assert sql.index("create_or_update_statistics") < sql.index("adapter.commit()")
    assert sql.index("post_snapshot(staging_table)") < sql.index("adapter.commit()")


def test_view_backup_is_removed_before_commit():
    sql = _read("models/view/view.sql")

    backup_drop = sql.index("adapter.drop_relation(backup_relation)")
    assert backup_drop < sql.rindex("adapter.commit()")


def test_hooks_no_longer_claim_transactions_are_unsupported():
    assert "don't support transactions" not in _read("hooks.sql")


def test_seed_reset_opens_transaction_before_mutating_existing_table():
    sql = _read("seeds/helpers.sql")

    begin = sql.index("auto_begin=True")
    assert begin < sql.index("adapter.drop_relation(old_relation)")
    assert begin < sql.index("adapter.truncate_relation(old_relation)")


def test_read_only_adapter_statements_do_not_open_transactions():
    read_only_statements = {
        "metadata.sql": [
            "list_schemas",
            "check_schema_exists",
            "list_relations_without_caching",
            "list_function_relations_without_caching",
            "last_modified",
        ],
        "columns.sql": ["get_columns_in_relation", "get_columns_in_query"],
        "relation.sql": ["find_references"],
        "indexes.sql": ["find_references", "list_nonclustered_rowstore_indexes"],
        "catalog.sql": ["catalog"],
        "freshness.sql": ["collect_freshness"],
    }

    for macro_path, statement_names in read_only_statements.items():
        sql = _read_adapter_macro(macro_path)
        for statement_name in statement_names:
            statement_call = re.search(
                rf"statement\(\s*'{statement_name}'.*?%\}}",
                sql,
                re.DOTALL,
            )
            assert statement_call is not None
            assert "auto_begin=False" in statement_call.group()


def test_read_only_utility_statements_do_not_open_transactions():
    read_only_statements = {
        "date_spine.sql": ["get_intervals_between"],
        "validate_sql.sql": [
            "set_showplan_on",
            "run_sql",
            "set_showplan_off",
        ],
    }

    for macro_path, statement_names in read_only_statements.items():
        sql = _read_utility_macro(macro_path)
        for statement_name in statement_names:
            statement_call = re.search(
                rf"statement\(\s*'{statement_name}'.*?%\}}",
                sql,
                re.DOTALL,
            )
            assert statement_call is not None
            assert "auto_begin=False" in statement_call.group()

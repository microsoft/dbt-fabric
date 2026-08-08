import abc
from typing import Any

from dbt.adapters.base import available
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabric.purview_sync import PurviewSync, extract_syncable_models
from dbt.adapters.sql.impl import SQLAdapter

logger = AdapterLogger("fabric")


class BaseFabricAdapter(SQLAdapter, metaclass=abc.ABCMeta):
    def _fetch_catalog_columns(self, models: list) -> dict[str, list[tuple[str, str]]]:
        """Fetch column names and data types from the database catalog for each model."""
        catalog_columns: dict[str, list[tuple[str, str]]] = {}
        for model in models:
            unique_id = model.get("unique_id", "")
            database = model.get("database", "")
            schema_name = model.get("schema", "")
            name = model.get("alias") or model.get("name", "")
            if not (database and schema_name and name):
                continue
            try:
                relation = self.Relation.create(
                    database=database, schema=schema_name, identifier=name, type="table"
                )
                cols = self.get_columns_in_relation(relation)
                catalog_columns[unique_id] = [(c.name, c.data_type) for c in cols]
            except Exception:
                logger.debug(f"Purview: could not fetch catalog columns for {unique_id}")
        return catalog_columns

    @available
    def purview_sync(
        self,
        graph: Any,
        results: Any = None,
        sync_descriptions: bool = True,
        sync_lineage: bool = True,
        sync_metadata: bool = True,
    ) -> str:
        """Sync dbt model metadata to Microsoft Purview.

        Callable from dbt macros via adapter.purview_sync(). Matches dbt models to Purview
        entities, then pushes descriptions, business metadata, and/or lineage depending on flags.
        """
        credentials = self.config.credentials
        if not credentials.purview_endpoint:
            logger.warning("Purview sync skipped: purview_endpoint not configured in profiles.yml")
            return ""

        client = self.connections.get_purview_client(credentials)
        fabric_client = self.connections.get_fabric_api_client(credentials)

        models = extract_syncable_models(graph, results)
        if not models:
            logger.info("Purview sync: no syncable models found")
            return ""

        catalog_columns = self._fetch_catalog_columns(models)
        sync = PurviewSync(client, fabric_client, graph, catalog_columns=catalog_columns)

        logger.info(f"Purview sync: syncing {len(models)} models")
        resolved = sync.resolve_entities(models)

        if not resolved:
            logger.warning("Purview sync: no models could be matched to Purview entities")
            return ""

        if sync_descriptions or sync_metadata:
            sync.push_metadata(
                models,
                resolved,
                results,
                sync_descriptions=sync_descriptions,
                sync_metadata=sync_metadata,
            )
        if sync_lineage:
            sync.push_lineage(models, resolved, is_full_sync=(results is None))

        logger.info("Purview sync completed")
        return ""

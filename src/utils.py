from pyapacheatlas.core import PurviewClient
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.core.typedef import (
    AtlasAttributeDef,
    EntityTypeDef,
    RelationshipTypeDef,
)
from pyapacheatlas.core import AtlasEntity
from pyspark.sql import DataFrame

import requests
import uuid


class PurviewPOCClient(PurviewClient):
    def __init__(self, account_name: str, authentication=None):
        self.account_name = account_name
        self.scan_endpoint_url = f"https://{account_name}.scan.purview.azure.com"
        self.catalog_endpoint_url = (
            f"https://{account_name}.purview.azure.com/catalog/api"
        )
        self.account_endpoint_url = f"https://{account_name}.purview.azure.com/account"
        super().__init__(account_name=account_name, authentication=authentication)

    def create_or_update_collection(
        self, collection_name: str, parent_collection: str = None
    ):
        url = self.account_endpoint_url + f"/collections/{collection_name}"

        createOrUpdateCollection = requests.put(
            url,
            headers=self.authentication.get_authentication_headers(),
            params={"api-version": "2019-11-01-preview"},
            json={
                "description": "test",
                "friendlyName": collection_name,
                "parentCollection": {
                    "type": "CollectionReference",
                    "referenceName": parent_collection
                    if parent_collection
                    else self.account_name,
                },
            },
        )

        return self._handle_response(createOrUpdateCollection)

    def register_Adls2_data_source(
        self, source_name: str, storage_account: str, collection_name: str = None
    ):
        url = self.scan_endpoint_url + f"/datasources/{source_name}"

        createOrUpdateDatasource = requests.put(
            url,
            headers=self.authentication.get_authentication_headers(),
            json={
                "kind": "AdlsGen2",
                "properties": {
                    "endpoint": f"https://{storage_account}.dfs.core.windows.net/",
                    "collection": {
                        "referenceName": collection_name
                        if collection_name
                        else self.account_name,
                        "type": "CollectionReference",
                    },
                },
            },
        )
        return self._handle_response(createOrUpdateDatasource)

    def create_or_update_scan(
        self, source_name: str, scan_name: str, collection_name: str
    ):
        url = self.scan_endpoint_url + f"/datasources/{source_name}/scans/{scan_name}"

        createOrUpdateScan = requests.put(
            url,
            headers=self.authentication.get_authentication_headers(),
            params={"api-version": "2018-12-01-preview"},
            json={
                "kind": "AdlsGen2Msi",
                "properties": {
                    "scanRulesetName": "AdlsGen2",
                    "scanRulesetType": "System",
                    "collection": {
                        "referenceName": collection_name,
                        "type": "CollectionReference",
                    },
                },
            },
        )
        return self._handle_response(createOrUpdateScan)

    def run_scan(
        self, source_name: str, scan_name: str, scan_level: str = "Incremental"
    ):
        if scan_level != "Full" and scan_level != "Incremental":
            raise ValueError("scan_level should be 'Full' or 'Incremental'")

        guid = uuid.uuid4()
        url = (
            self.scan_endpoint_url
            + f"/datasources/{source_name}/scans/{scan_name}/runs/{guid}"
        )

        runScan = requests.put(
            url,
            headers=self.authentication.get_authentication_headers(),
            params={"api-version": "2018-12-01-preview", "scanLevel": scan_level},
        )

        return self._handle_response(runScan)

    def create_delta_table_typedefs(self):

        type_delta_table_df = EntityTypeDef(
            name="custom_delta_table",
            attributeDefs=[AtlasAttributeDef(name="format")],
            superTypes=["DataSet"],
            options={"schemaElementAttribute": "columns"},
        )

        type_delta_table_columns = EntityTypeDef(
            name="custom_delta_table_column",
            attributeDefs=[AtlasAttributeDef(name="data_type")],
            superTypes=["DataSet"],
        )

        type_spark_job = EntityTypeDef(
            name="custom_spark_job_process",
            attributeDefs=[
                AtlasAttributeDef(name="job_type", isOptional=False),
                AtlasAttributeDef(name="schedule", defaultValue="adHoc"),
            ],
            superTypes=["Process"],
        )

        spark_column_to_df_relationship = RelationshipTypeDef(
            name="custom_delta_table_to_columns",
            relationshipCategory="COMPOSITION",
            endDef1={
                "type": "custom_delta_table",
                "name": "columns",
                "isContainer": True,
                "cardinality": "SET",
                "isLegacyAttribute": False,
            },
            endDef2={
                "type": "custom_delta_table_column",
                "name": "delta_table",
                "isContainer": False,
                "cardinality": "SINGLE",
                "isLegacyAttribute": False,
            },
        )

        return self.upload_typedefs(
            entityDefs=[type_delta_table_df, type_delta_table_columns, type_spark_job],
            relationshipDefs=[spark_column_to_df_relationship],
            force_update=True,
        )

    def register_df(self, df: DataFrame, name: str, qualified_name: str):
        colEntities = []
        guid_tracker = GuidTracker()

        ts = AtlasEntity(
            name="demoDFSchema",
            typeName="tabular_schema",
            qualified_name=f"{qualified_name}_tabular_schema",
            guid=guid_tracker.get_guid(),
        )

        for (col, type) in df.dtypes:
            colEntities.append(
                AtlasEntity(
                    name=col,
                    typeName="column",
                    qualified_name=f"{qualified_name}_column_{col}",
                    guid=guid_tracker.get_guid(),
                    attributes={
                        "type": type,
                        "description": f"Column {col} has type {type}",
                    },
                    relationshipAttributes={"composeSchema": ts.to_json(minimum=True)},
                )
            )

        rs = AtlasEntity(
            name=name,
            typeName="azure_datalake_gen2_resource_set",
            qualified_name=qualified_name,
            guid=guid_tracker.get_guid(),
            relationshipAttributes={"tabular_schema": ts.to_json(minimum=True)},
        )

        return (rs, ts, colEntities)

    def register_lineage(self, input, output):
        return

from pyapacheatlas.core import PurviewClient
from pyapacheatlas.core.util import GuidTracker
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


if __name__ == "__main__":

    from dotenv import load_dotenv
    from pyapacheatlas.auth import ServicePrincipalAuthentication
    import os

    load_dotenv()

    tenant_id = os.environ.get("TENANT_ID")
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    account_name = os.environ.get("PURVIEW_ACCOUNT")
    storage_account = os.environ.get("STORAGE_ACCOUNT")

    auth = ServicePrincipalAuthentication(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    scan_name = "MyScan"
    collection_name = "MyCollection"
    source_name = "MyAdls2Source"

    client = PurviewPOCClient(account_name=account_name, authentication=auth)
    response = client.create_or_update_collection(collection_name=collection_name)
    print(response)

    response = client.register_Adls2_data_source(
        source_name=source_name,
        storage_account=storage_account,
        collection_name=collection_name,
    )
    print(response)

    response = client.create_or_update_scan(
        source_name=source_name, scan_name=scan_name, collection_name=collection_name
    )
    print(response)
    response = client.run_scan(
        source_name=source_name, scan_name=scan_name, scan_level="Incremental"
    )
    print(response)

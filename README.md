# Purview / Atlas POC

This repo contains a minimal example of how to create custom Entity types in Azure Purview (a managed service built on top of Apache Atlas).

At the time of writing, Azure does not have an SDK for Purview. As recommended by the [offical Azure documentation](https://www.youtube.com/watch?v=4qzjnMf1GN4), the PyApacheAtlas library can be used to interact with the Atlas API exposed by Purview. 

## Setup 

The infrastructure is setup with Terraform and can be found in the `infrastructure` directory. It contains a resource group and a Purview and storage account within that resource group. In addition, it contains an Azure AD Application and associated Service Principal which are used to programmatically access the Atlas API. The outputs of the Terraform root module are the `client_id`, `client_secret`, `tenant_id` and `atlas_endpoint` that are required to access the API. 

Note that you cannot create the role assignments that are needed for your Service Principal (SP) in the Purview Data Plane with Terraform (yet). To give your SP access, you'll have to click your way through the Purview UI as described in the [official docs](https://docs.microsoft.com/en-us/azure/purview/tutorial-using-rest-apis). 

You can create role assignments for the SP for the data sources that you want to access (another way would be to link a Key Vault (KV) and then use the secrets within the KV for gaining access). 

## Data source scanning and discovery

Probably the easiest way to get started with Purview is to scan pre-existing data sources 

You can also trigger scans programmatically 

## CRUD of (custom) resources with the Atlas API

- Lineage of Spark jobs: [docs](https://docs.microsoft.com/en-gb/azure/purview/how-to-lineage-spark-atlas-connector) not supported for Spark 3.0 ([hortonworks connector](https://github.com/hortonworks-spark/spark-atlas-connector), no longer under development)

## Lineage via Azure Data Factory

When data sets in Azure Data Factory are transformed in a Pipeline (Synapse Analytics)

## Access control (private preview)

[documentation](https://docs.microsoft.com/en-gb/azure/purview/how-to-access-policies-storage)

at folder or file level granularity. The documentation seems to suggest that you can use the classification attribute of a data resource to specify whether to apply a policy or not. 

## Pricing

$0.4/h for infra (per 2GB of metadata storage), pay extra for scanning, 

So on a yearly basis, estimate of about $3500 fixed cost plus variable scanning costs. 
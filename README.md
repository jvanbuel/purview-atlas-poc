# Purview / Atlas POC

This repo contains the result of an investigation of the Azure Purview service, a managed data catalog solution built on top of [Apache Atlas](https://atlas.apache.org/#/). In summary, it contains code that showcases

- how to set up Azure Purview with Terraform and configure it with ancillary services such as Key Vault and Data Factory
- how to use a service principal to access the Purview API endpoints (mainly `catalog` and `scan`) programmatically, e.g. for initiating a data source scan from within a pipeline (as opposed to via preconfigured triggers clicked together in the Purview UI)
- how to create custom Entity types in Azure Purview

A good introduction to the Purview REST APIs is a [set of LinkedIn posts](https://www.linkedin.com/pulse/azure-purview-rest-api-part-1-getting-started-raihan-alam) by a Solution Architect at Microsoft. At the time of writing, Azure does not have an SDK for Purview. As recommended by the [offical Azure documentation](https://www.youtube.com/watch?v=4qzjnMf1GN4), the PyApacheAtlas library can be used to interact with the Atlas API exposed by Purview. For the other APIs (e.g. scan) this repo makes use of the `requests` python library.

## Setup 

The infrastructure can be found in the `infrastructure` directory. It contains a resource group and a Purview and storage account within that resource group. In addition, it contains an Azure AD Application and associated Service Principal which are used to programmatically access the Atlas API. The outputs of the Terraform root module are the `client_id`, `client_secret`, `tenant_id` and `atlas_endpoint` that are required to access the API, as well as the `storage_account` name and `storage_key`. 

Note that you cannot create the role assignments that are needed for your Service Principal (SP) in the Purview Data Plane with Terraform (yet). To give your SP access, you'll have to click your way through the Purview UI as described in the [official docs](https://docs.microsoft.com/en-us/azure/purview/tutorial-using-rest-apis). 


The identity resource attribute of the Purview account is that of the Purview Managed System Identity (MSI) that automatically gets created for each account, and can be used to declaratively define access to resources that you would like to scan with Purview (storage accounts, databases, Power BI, ...). Alternatively, you could link a Key Vault (KV) to the Purview account and then use the secrets within the KV for gaining access to your data sources.

## Metadata ingestion and discovery

### Data source scanning 

Probably the easiest way to get started with Purview is to let Purview scan pre-existing data sources. A [list of supported data sources](https://docs.microsoft.com/en-us/azure/purview/purview-connector-overview) can be found in the official documentation. As mentioned in the setup section, you can create access roles for the MSI, but for non-Azure (or third party) data sources, you'll have to provide access to those data sources by storing their credentials in a Key Vault and linking that Key Vault to your Purview account. 

Although you can schedule data source scans or trigger them on an ad hoc basis via the Purview UI, you probably want to ingest your data's metadata as soon as it gets created in order to avoid stale metadata. To achieve this, you can trigger scans programmatically from within your data pipelines via the Atlas REST API. This is not (yet?) supported by the PyApacheAtlas library, as it is custom to Purview and not part of the Apache Atlas project. 

An example of such a programmatically triggered scan can be found in the main pipeline in `src/app.py`. This pipeline processes some publicly available vaccine and country data stored in the `data` folder and uploads it to the storage account created by Terraform. By setting the environment variable `SCAN=1` in the `.env` file, the pipeline triggers a Purview scan after the upload hass completed. Make sure to also properly configure the other environment variables in the `.env` file, which are needed to authenticate and authorize access against the Purview API and the storage account. All these variables should be availabe in the `terraform output` of the root module. 

### Lineage via Azure Data Factory (and Synapse Analytics)

A data catalog worth its salt not only gives you an overview of your data, but also tells you where the data originated. In Purview, automatic lineage detection of data sets is supported for Copy, Data Flow, and Execute SSIS activities in Azure Data Factory (or Synapse Analytics) pipelines.

The service connection between Data Factory and Purview needs to be set up manually and is not stored in the Resource Manager templates created by Data Factory (see [docs](https://docs.microsoft.com/en-us/azure/data-factory/connect-data-factory-to-azure-purview) on how to setup the connection). 

The `data_factory` folder contains the Azure Resource Manager (ARM) templates of the Data Factory datasets of the example pipeline of the previous section. It also contains an ARM template of a Data Factory pipeline that converts the parquet file in the `master` directory of the datalake to a csv file. The Data Factory defined in the Terraform code automatically applies these ARM templates upon creation. After setting up the connection between Data Factory and Purview, and triggering the pipeline in Data Factory, the lineage of this pipeline will be discoverable in your Purview account. 

There exists a [custom Atlas connector](https://github.com/hortonworks-spark/spark-atlas-connector) for lineage of Spark jobs, developed by Hortonworks, but it seems that the project is no longer actively maintained. There is still a [tutorial](https://docs.microsoft.com/en-gb/azure/purview/how-to-lineage-spark-atlas-connector) on how to configure this connector in the Purview docs, but the last supported Spark version is 2.4. To document lineage for other data sources, the only option is to create custom entities via the Atlas API.

## CRUD of custom resources and lineage with the Atlas API


To add data sources which are not (yet) supported natively by Azure Purview, you can directly access the Atlas API.  


## Access control (private preview)

During Microsoft Ignite 2021, a new feature for Purview to provision data access policies for Azure Storage was announced. With this new feature, you can limit access to resources in your Storage Accounts at the folder or file level.

A screenshot in the [documentation](https://docs.microsoft.com/en-gb/azure/purview/how-to-access-policies-storage) seems to suggest that you can use the classification attribute of a data resource to specify whether to apply a policy or not, but this feature is not showcased in the corresponding [demo video](https://www.youtube.com/watch?v=CFE8ltT19Ss). 

The Purview documentation on this new access policy feature also hints at a new feature to manage fine-grained access control on your data assets in Azure Storage: [tag-based RBAC roles](https://docs.microsoft.com/en-us/azure/role-based-access-control/conditions-overview). While still in preview, this could provide a powerful and more easily manageable alternative to ACLs. 

## Pricing

The cost of a Purview account is $0.4/h for infrastructure per 2GB of metadata storage. You pay extra for scanning data sources, wich uses 32 vCores at $0.6 per vCore per hour. If you have partitioned datasets, you can enable the `Resoure sets` feature which optimizes storage and search of these partitions at an additional cost. 

So on a yearly basis, there is a $3.5k per 2GB of metadata fixed cost plus variable scanning costs. Based on a conservative guess of 1-2h scanning time per day for a moderately sized data lake, the variable costs would add another $7-15k rendering a $10-20k total yearly cost. 

## Closing thoughts

Some unstructured thoughts after trying out Purview: 

- Data source scans do not produce (column-level) statistics of the discovered datasets, which would be useful to detect data quality issues. You can of course add this metadata by calculating the statistics yourself and add it as an attribute when creating a custom entity, but it would be nice if this was included out of the box with (at least some of) the supported data sources (cfr. LinkedIn's [DataHub](https://datahubproject.io/)). 
- It is not very practical to write your own data source connector (again, cfr. [DataHub](https://datahubproject.io/docs/metadata-ingestion/adding-source)). It appears that there is not really a community for custom connectors. 
- Signing up to the private preview of the Purview data access policy feature takes a long time to approve (over two weeks, no reply yet).
- Automatic lineage support is quite rudimentary. I would still like to try out the [Airflow lineage integration with Apache Atlas](https://airflow.apache.org/docs/apache-airflow/1.10.4/lineage.html).
- You can do a lot of custom things with PyApacheAtlas and the Purview scan REST API, but having a unified Purview SDK would be useful.


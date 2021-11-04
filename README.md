# Purview / Atlas POC

This repo contains a minimal example of how to create custom Entity types in Azure Purview (a managed service built on top of Apache Atlas).

At the time of writing, Azure does not have an SDK for Purview. As recommended by the offical Azure documentation, the PyApacheAtlas library is used to interact with the Atlas API exposed by Purview. 

The infrastructure is setup with Terraform and can be found in the `infrastructure` directory. It contains a resource group and a Purview and storage account within that resource group. In addition, it contains an Azure AD Application and associated Service Principal which are used to programmatically access the Atlas API. The outputs of the Terraform root module are the `client_id`, `client_secret`, `tenant_id` and `atlas_endpoint` that are required to access the API. 
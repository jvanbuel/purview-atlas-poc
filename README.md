# Purview / Atlas POC

This repo contains a minimal example of how to create custom Entity types in Azure Purview (a managed service built on top of Apache Atlas).

At the time of writing, Azure does not have an SDK for Purview. As recommended by the [offical Azure documentation](https://www.youtube.com/watch?v=4qzjnMf1GN4), the PyApacheAtlas library can be used to interact with the Atlas API exposed by Purview. 

The infrastructure is setup with Terraform and can be found in the `infrastructure` directory. It contains a resource group and a Purview and storage account within that resource group. In addition, it contains an Azure AD Application and associated Service Principal which are used to programmatically access the Atlas API. The outputs of the Terraform root module are the `client_id`, `client_secret`, `tenant_id` and `atlas_endpoint` that are required to access the API. 

Note that you cannot create the role assigmnents that are needed for your Service Principal (SP) in the Purview Data Plane. To give your SP access, you'll have to click your way through the Purview UI as described in the [official docs](https://docs.microsoft.com/en-us/azure/purview/tutorial-using-rest-apis). 
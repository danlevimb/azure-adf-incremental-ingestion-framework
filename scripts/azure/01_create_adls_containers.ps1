<#
===============================================================================
Script: 01_create_adls_containers.ps1
Purpose:
    Creates the ADLS Gen2 containers used by the ADF Incremental Ingestion
    Framework.

Notes:
    - This script is parameterized for public repository use.
    - Replace the variables with your own Azure resource values.
    - Do not commit account keys, secrets, or connection strings.
===============================================================================
#>

$resourceGroupName = "<RESOURCE_GROUP_NAME>"
$storageAccountName = "<STORAGE_ACCOUNT_NAME>"

$containers = @(
    "landing",
    "bronze",
    "rejected",
    "metadata",
    "evidence"
)

Write-Host "Getting storage account key..." -ForegroundColor Cyan

$storageAccountKey = az storage account keys list `
    --resource-group $resourceGroupName `
    --account-name $storageAccountName `
    --query "[0].value" `
    --output tsv

foreach ($container in $containers) {
    Write-Host "Creating container: $container" -ForegroundColor Green

    az storage container create `
        --account-name $storageAccountName `
        --account-key $storageAccountKey `
        --name $container `
        --output table
}

Write-Host "Container creation completed." -ForegroundColor Cyan
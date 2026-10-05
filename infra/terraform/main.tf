# Minimal Azure footprint for the MDM platform (illustrative - module inputs trimmed for brevity).
# terraform apply -var env=prod
terraform {
  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 4.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
  }
}
provider "azurerm" { features {} }

variable "env"      { type = string }
variable "location" { default = "centralindia" }

resource "azurerm_resource_group" "mdm" {
  name     = "rg-mdm-${var.env}"
  location = var.location
}

# ADLS Gen2: landing + lakehouse containers, hierarchical namespace, private endpoints in prod
resource "azurerm_storage_account" "lake" {
  name                            = "stmdm${var.env}"
  resource_group_name             = azurerm_resource_group.mdm.name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = var.env == "prod" ? "ZRS" : "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  public_network_access_enabled   = var.env != "prod"
  allow_nested_items_to_be_public = false
}
resource "azurerm_storage_container" "landing"   { name = "landing"   storage_account_id = azurerm_storage_account.lake.id }
resource "azurerm_storage_container" "lakehouse" { name = "lakehouse" storage_account_id = azurerm_storage_account.lake.id }

# Lifecycle: raw landing files move to cool after 30 days, archive after 180 (cost)
resource "azurerm_storage_management_policy" "tiering" {
  storage_account_id = azurerm_storage_account.lake.id
  rule {
    name    = "landing-tiering"
    enabled = true
    filters { blob_types = ["blockBlob"] prefix_match = ["landing/"] }
    actions { base_blob { tier_to_cool_after_days_since_modification_greater_than = 30
                          tier_to_archive_after_days_since_modification_greater_than = 180 } }
  }
}

resource "azurerm_key_vault" "kv" {
  name                      = "kv-mdm-${var.env}"
  resource_group_name       = azurerm_resource_group.mdm.name
  location                  = var.location
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  sku_name                  = "standard"
  purge_protection_enabled  = true
  enable_rbac_authorization = true
}
data "azurerm_client_config" "current" {}

resource "azurerm_databricks_workspace" "dbx" {
  name                = "dbw-mdm-${var.env}"
  resource_group_name = azurerm_resource_group.mdm.name
  location            = var.location
  sku                 = "premium"            # Unity Catalog, column masks, audit logs
}

resource "azurerm_data_factory" "adf" {
  name                = "adf-mdm-${var.env}"
  resource_group_name = azurerm_resource_group.mdm.name
  location            = var.location
  identity { type = "SystemAssigned" }      # managed identity -> no secrets for ADLS access
}

resource "azurerm_log_analytics_workspace" "logs" {
  name                = "log-mdm-${var.env}"
  resource_group_name = azurerm_resource_group.mdm.name
  location            = var.location
  retention_in_days   = 90
}

resource "azurerm_eventhub_namespace" "events" {
  name                = "evh-mdm-${var.env}"
  resource_group_name = azurerm_resource_group.mdm.name
  location            = var.location
  sku                 = "Standard"
  capacity            = 1
}

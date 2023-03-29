resource "azurerm_storage_account" "example" {
  name                     = "wu4synapsestorage"
  resource_group_name      = "wu4group"
  location                 = "East US"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "example" {
  name               = "wu4gen2"
  storage_account_id = azurerm_storage_account.example.id
}

resource "azurerm_synapse_workspace" "example" {
  name                                 = "wu4synapse-workspace"
  resource_group_name                  = "wu4group"
  location                             = "East US"
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.example.id
  sql_administrator_login              = "wu4user"
  sql_administrator_login_password     = "Wu4password"

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_synapse_sql_pool" "example" {
  name                 = "wu4datawarehouse"
  synapse_workspace_id = azurerm_synapse_workspace.example.id
  sku_name             = "DW100c"
  create_mode          = "Default"
}
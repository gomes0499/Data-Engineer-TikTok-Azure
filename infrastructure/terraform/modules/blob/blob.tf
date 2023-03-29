resource "azurerm_storage_container" "example" {
  name                  = "datalake"
  storage_account_name  = "wu4storage"
  container_access_type = "container"
}

resource "azurerm_storage_blob" "raw" {
  name                   = "raw/"
  storage_account_name   = "wu4storage"
  storage_container_name = azurerm_storage_container.example.name
  type                   = "Block"
  content_type           = "application/json"
}

resource "azurerm_storage_blob" "process" {
  name                   = "process/"
  storage_account_name   = "wu4storage"
  storage_container_name = azurerm_storage_container.example.name
  type                   = "Block"
  content_type           = "application/parquet"
}



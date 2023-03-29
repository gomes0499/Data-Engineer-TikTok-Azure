terraform {
  backend "azurerm" {
    resource_group_name  = "wu4group"
    storage_account_name = "wu4storage"
    container_name       = "wu4tfstate"
    key                  = "terraform.tfstate"
  }
}

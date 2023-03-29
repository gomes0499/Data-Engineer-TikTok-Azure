resource "azurerm_cognitive_account" "example" {
  name                = "wu4-cognitive-account"
  location            = "East US"
  resource_group_name = "wu4group"
   kind               = "TextAnalytics"

  sku_name = "F0"

  tags = {
    Acceptance = "Test"
  }
}

output "text_analytics_key" {
  value       = azurerm_cognitive_account.example.primary_access_key
  description = "The primary access key for the Text Analytics instance."
  sensitive   = true
}

output "text_analytics_endpoint" {
  value       = azurerm_cognitive_account.example.endpoint
  description = "The endpoint URL for the Text Analytics instance."
}
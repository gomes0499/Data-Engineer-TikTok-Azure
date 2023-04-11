resource "azurerm_eventhub_namespace" "example" {
  name                = "wu4eventhub-namespace"
  location            = "East US"
  resource_group_name = "wu4group"
  sku                 = "Standard"
  capacity            = 1

  tags = {
    environment = "Development"
  }
}

resource "azurerm_eventhub" "example" {
  name                = "wu4eventhub"
  namespace_name      = azurerm_eventhub_namespace.example.name
  resource_group_name = "wu4group"
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub_consumer_group" "example" {
  name                = "wu4eventconsumer"
  namespace_name      = azurerm_eventhub_namespace.example.name
  eventhub_name       = azurerm_eventhub.example.name
  resource_group_name = "wu4group"
}

resource "azurerm_stream_analytics_job" "example" {
  name                                     = "wu4streamjob"
  resource_group_name                      = "wu4group"
  location                                 = "East US"
  compatibility_level                      = "1.2"
  data_locale                              = "en-GB"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 50
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  streaming_units                          = 3
    tags = {
    environment = "Example"
  }

  transformation_query = <<QUERY
    SELECT *
    INTO [wu4-output-to-blob-storage]
    FROM [wu4-eventhub-stream-input]
QUERY

}

resource "azurerm_stream_analytics_stream_input_eventhub" "example" {
  name                         = "wu4-eventhub-stream-input"
  resource_group_name          = "wu4group"
  stream_analytics_job_name    = "wu4streamjob"
  eventhub_consumer_group_name = azurerm_eventhub_consumer_group.example.name
  eventhub_name                = azurerm_eventhub.example.name
  servicebus_namespace         = azurerm_eventhub_namespace.example.name
  shared_access_policy_key     = azurerm_eventhub_namespace.example.default_primary_key
  shared_access_policy_name    = "RootManageSharedAccessKey"

  serialization {
    type     = "Json"
    encoding = "UTF8"
  }
}

resource "azurerm_stream_analytics_output_blob" "example" {
  name                      = "wu4-output-to-blob-storage"
  stream_analytics_job_name = "wu4streamjob"
  resource_group_name       = "wu4group"
  storage_account_name      = "wu4storage"
  storage_account_key       = "*"
  storage_container_name    = "datalake"
  path_pattern              = "raw/{date}/{time}"
  date_format               = "yyyy-MM-dd"
  time_format               = "HH-mm"
  depends_on = [azurerm_stream_analytics_job.example]

  serialization {
    type            = "Json"
    encoding        = "UTF8"
    format          = "LineSeparated"
  }
}

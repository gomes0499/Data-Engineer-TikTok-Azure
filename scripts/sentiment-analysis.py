import configparser
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/4-Project/config/config.ini")

account_url = "https://wu4storage.blob.core.windows.net"
default_credential = DefaultAzureCredential()

TEXT_ANALYTICS_KEY = config.get("textanalytics", "TEXT_ANALYTICS_KEY")
TEXT_ANALYTICS_ENDPOINT = config.get("textanalytics", "TEXT_ANALYTICS_ENDPOINT")

RAW_FOLDER_PATH = "raw/"
PROCESSED_FOLDER_PATH = "process/"
STORAGE_CONNECTION_STRING = config.get("blob", "STORAGE_CONNECTION_STRING")

blob_service_client = BlobServiceClient(account_url, credential=default_credential)

def process_text_data(data, text_column):
    text_analytics_client = TextAnalyticsClient(endpoint=TEXT_ANALYTICS_ENDPOINT, credential=AzureKeyCredential(TEXT_ANALYTICS_KEY))
    document_list = data[text_column].tolist()

    def process_batch(batch):
        return text_analytics_client.analyze_sentiment(batch)

    document_batch = [{"id": str(i), "text": text} for i, text in enumerate(document_list)]
    batch_size = 10
    sentiments = []

    for i in range(0, len(document_batch), batch_size):
        batch = document_batch[i:i + batch_size]
        batch_sentiments = process_batch(batch)
        sentiments.extend(batch_sentiments)

    sentiment_data = [
        {
            "text": data.loc[data.index[i], text_column],
            "sentiment": sentiments[i].sentiment,
            "confidence_scores": {
                "positive": sentiments[i].confidence_scores.positive,
                "neutral": sentiments[i].confidence_scores.neutral,
                "negative": sentiments[i].confidence_scores.negative
            }
        }
        for i in range(len(sentiments))
    ]
    sentiment_df = pd.DataFrame(sentiment_data)
    return pd.concat([data.reset_index(drop=True), sentiment_df.drop(columns='text')], axis=1)

def main():
    container_client = ContainerClient.from_connection_string(STORAGE_CONNECTION_STRING, "datalake")
    blobs = container_client.list_blobs(name_starts_with=RAW_FOLDER_PATH)

    combined_df = None

    for blob in blobs:
        raw_blob_client = container_client.get_blob_client(blob.name)
        raw_content = raw_blob_client.download_blob().readall().decode('utf-8')
        json_objects = list(filter(None, raw_content.split('\n')))
        df_list = [pd.DataFrame(json.loads(json_str), index=[0]) for json_str in json_objects]

        if not df_list:
            continue

        input_data = pd.concat(df_list, ignore_index=True)
        print(input_data.head())

        # Separate video and comment data
        video_data = input_data[input_data['event_type'] == 'video']
        comment_data = input_data[input_data['event_type'] == 'comment']

        # Process video descriptions
        if not video_data.empty:
            video_data = process_text_data(video_data, 'description')

        # Process comment text
        if not comment_data.empty:
            comment_data = process_text_data(comment_data, 'comment_text')

        # Combine the processed video and comment data
        merged_data = pd.concat([video_data, comment_data], ignore_index=True)

        if combined_df is None:
            combined_df = merged_data
        else:
            combined_df = pd.concat([combined_df, merged_data], ignore_index=True)

    output_parquet_path = f"{PROCESSED_FOLDER_PATH}output.parquet"
    parquet_blob_client = container_client.get_blob_client(output_parquet_path)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pandas(combined_df), parquet_buffer)
    parquet_blob_client.upload_blob(parquet_buffer.getvalue().to_pybytes(), overwrite=True)

if __name__ == "__main__":
    main()

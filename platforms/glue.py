import boto3
from platforms.base import TableCreator


class GlueTableCreator(TableCreator):

    def __init__(self, region="us-east-1"):
        self.client = boto3.client("glue", region_name=region)

    def create_table(self, table):
        columns = []
        for col in table.columns:
            columns.append({
                "Name": col.name,
                "Type": col.data_type.lower()
            })

        self.client.create_table(
            DatabaseName=table.database,
            TableInput={
                "Name": table.table_name,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"s3://your-bucket/{table.table_name}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    }
                },
                "TableType": "EXTERNAL_TABLE"
            }
        )

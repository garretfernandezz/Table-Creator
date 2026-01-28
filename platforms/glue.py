import boto3
from platforms.base import TableCreator
from botocore.exceptions import ClientError


class GlueTableCreator(TableCreator):
    def __init__(self, region_name="us-east-1", s3_base_path="s3://my-glue-data-lake"):
        self.client = boto3.client("glue", region_name=region_name)
        self.s3_base_path = s3_base_path

    def _ensure_database_exists(self, database_name):
        try:
            self.client.get_database(Name=database_name)
        except self.client.exceptions.EntityNotFoundException:
            print(f"Glue database '{database_name}' not found. Creating it...")
            self.client.create_database(
                DatabaseInput={"Name": database_name}
            )

    def create_table(self, table):
        database_name = table.namespace
        table_name = table.table_name

        # ✅ Ensure database exists
        self._ensure_database_exists(database_name)

        columns = [
            {
                "Name": c.name,
                "Type": c.data_type.lower(),
                "Comment": c.comment or ""
            }
            for c in table.columns
        ]

        table_input = {
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": f"{self.s3_base_path}/{database_name}/{table_name}/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    "Parameters": {"field.delim": ","}
                },
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE"}
        }

        try:
            self.client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"Glue table created: {database_name}.{table_name}")
        except self.client.exceptions.AlreadyExistsException:
            print(f"Glue table already exists: {database_name}.{table_name}")

from parsers.csv_parser import parse_schema_csv
from platforms.databricks import DatabricksTableCreator
from platforms.glue import GlueTableCreator


def main(csv_path, platform):
    tables = parse_schema_csv(csv_path)

    if platform == "databricks":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        creator = DatabricksTableCreator(spark)

    elif platform == "glue":
        creator = GlueTableCreator()

    else:
        raise ValueError("Unsupported platform")

    for table in tables:
        creator.create_table(table)

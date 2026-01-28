import sys
from table_creator.parsers.csv_parser import parse_schema_csv
from table_creator.platforms.databricks import DatabricksTableCreator
from table_creator.platforms.glue import GlueTableCreator

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

if __name__ == "__main__":
    csv_path = sys.argv[1]
    platform = sys.argv[2]

    main(csv_path, platform)

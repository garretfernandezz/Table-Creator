from table_creator.platforms.base import TableCreator


class DatabricksTableCreator(TableCreator):

    def __init__(self, spark):
        self.spark = spark

    def create_table(self, table):
        cols = []
        for col in table.columns:
            nullable = "" if col.nullable else "NOT NULL"
            cols.append(f"{col.name} {col.data_type} {nullable}")

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table.database}.{table.table_name} (
            {", ".join(cols)}
        )
        USING DELTA
        """

        self.spark.sql(ddl)

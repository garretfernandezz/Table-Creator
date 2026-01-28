from platforms.base import TableCreator


class DatabricksTableCreator(TableCreator):

    def __init__(self, spark):
        self.spark = spark

    def create_table(self, table):
        cols = []
        for col in table.columns:
            nullable = "" if col.nullable else "NOT NULL"
            cols.append(f"{col.name} {col.data_type} {nullable}")

        # Split namespace → catalog + schema
        try:
            catalog, schema = table.namespace.split(".")
        except ValueError:
            raise ValueError(
                f"Databricks requires namespace as 'catalog.schema', got '{table.namespace}'"
            )

        # Ensure schema exists
        self.spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
        )

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table.table_name} (
            {", ".join(cols)}
        )
        USING DELTA
        """

        self.spark.sql(ddl)

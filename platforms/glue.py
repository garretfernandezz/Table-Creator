from platforms.base import TableCreator


class GlueTableCreator(TableCreator):

    def create_table(self, table):
        database = table.namespace  # single-level

        # Glue-specific create table logic
        print(f"Creating Glue table {database}.{table.table_name}")

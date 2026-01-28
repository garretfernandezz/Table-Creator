class Table:
    def __init__(self, namespace, table_name):
        """
        namespace:
          - Databricks: "catalog.schema"
          - Glue: "database"
        """
        self.namespace = namespace
        self.table_name = table_name
        self.columns = []

    def add_column(self, column):
        self.columns.append(column)

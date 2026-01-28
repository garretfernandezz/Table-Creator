class Table:
    def __init__(self, database, table_name):
        self.database = database
        self.table_name = table_name
        self.columns = []

    def add_column(self, column):
        self.columns.append(column)

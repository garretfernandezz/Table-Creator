class Column:
    def __init__(self, name, data_type, nullable, primary_key, comment=None):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.primary_key = primary_key
        self.comment = comment

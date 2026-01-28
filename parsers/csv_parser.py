import csv
from models.table import Table
from models.column import Column


def parse_schema_csv(file_path):
    tables = {}

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["database"], row["table_name"])

            if key not in tables:
                tables[key] = Table(
                    database=row["database"],
                    table_name=row["table_name"]
                )

            column = Column(
                name=row["column_name"],
                data_type=row["data_type"],
                nullable=row["nullable"].lower() == "true",
                primary_key=row["primary_key"].lower() == "true",
                comment=row.get("comment")
            )

            tables[key].add_column(column)

    return list(tables.values())



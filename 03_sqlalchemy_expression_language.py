import pandas as pd
import sqlalchemy

import utils

# SqlAlchemy Query Language:
# - is built on MetaData
# - uses Method Chaining

metadata = sqlalchemy.MetaData()

# Define Tables & add them to metadata:
user_table = sqlalchemy.Table(
    "user",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer(), primary_key=True, autoincrement=True),
    sqlalchemy.Column("first_name", sqlalchemy.String(), nullable=False),
    sqlalchemy.Column("last_name", sqlalchemy.String(), nullable=False),
    sqlalchemy.Column("age", sqlalchemy.Integer(), nullable=False),
)

# Create Engine:
engine = sqlalchemy.create_engine("sqlite:///:memory:")

# Create connection, start transaction and create Tables:
with engine.begin() as con:
    metadata.create_all(con)
    print(f"* Inserting following Csv data...")
    df = pd.read_csv("user.csv", header=0)
    print(df)
    df.to_sql(
        # target table name:
        "user",
        con,
        # ignore index i.e. no extra column just for the index
        index=False,
        # "append" to existing table
        # alternatives:
        # "fail", if table already exists
        # "replace, if table already exists
        if_exists="append")

utils.print_table(engine, "user")

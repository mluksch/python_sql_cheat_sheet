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
engine = sqlalchemy.create_engine("sqlite:///:memory:", echo=False, future=True)

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

########## Creating SQL-Statements with the Table-object ##########
print("Creating SQL-Statements with the Table-object:")

# Where-Clause
where_clause = sqlalchemy.or_(user_table.c.first_name == "Jerome", user_table.c.last_name == "Beaver")
print(f"* where_clause: {where_clause}")

# Select-Clause
select_clause = sqlalchemy.select([user_table.c.first_name, user_table.c.last_name, user_table.c.age]).order_by(
    user_table.c.age)
print(f"* select_clause: {select_clause}")

select_clause_all = sqlalchemy.select([user_table])
print(f"* select_clause: {select_clause}")

# Build complete SQL-Queries with Method-chaining:
# The SQL-From-Clause is not required in SQL-Alchemy but implicitly set

# (1) Read-Query:
sql_stmt = select_clause.order_by(user_table.c.age).where(where_clause)
print(f"* Read-Query sql_stmt: {sql_stmt}")
with engine.connect() as con:
    row = con.execute(sql_stmt).fetchone()
    print(f"* sql_stmt result: {row}")

# (2.a) Insert with a dict:
sql_stmt = user_table.insert()
print(f"* Insert-Statement sql_stmt: {sql_stmt}")
with engine.begin() as con:
    con.execute(sql_stmt, {"first_name": "Jerry", "last_name": "Berry", "age": 19})
utils.print_table(engine, "user")

# (2.b) insert with named parameters:
sql_stmt = user_table.insert().values(first_name="Tom", last_name="Tom", age=14)
with engine.begin() as con:
    con.execute(sql_stmt)
utils.print_table(engine, "user")

# (2.c) insert a list of dicts at once:
sql_stmt = user_table.insert()
with engine.begin() as con:
    con.execute(sql_stmt, [
        {"first_name": "Jack", "last_name": "Captain", "age": 12},
        {"first_name": "Jack", "last_name": "Black", "age": 23},
        {"first_name": "Jack", "last_name": "Track", "age": 34},
    ])
utils.print_table(engine, "user")

# (3.a) update with a dictionary:
sql_stmt = user_table.update().where(user_table.c.last_name == "Beaver")
print(f"* Update-Statement sql_stmt: {sql_stmt}")
with engine.begin() as con:
    con.execute(sql_stmt, {"first_name": "Sigourney", "age": 65})
utils.print_table(engine, "user")

# (3.b) update with a named parameters:
sql_stmt = user_table.update().where(user_table.c.last_name == "Tennis").values(first_name="Boris", age=44)
sql_stmt = user_table.update().values(first_name="Gerd", age=44).where(user_table.c.last_name == "Nerd")
print(f"* Update-Statement sql_stmt: {sql_stmt}")
with engine.begin() as con:
    con.execute(sql_stmt)
utils.print_table(engine, "user")

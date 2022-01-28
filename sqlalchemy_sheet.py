################  Read-Tutorials  ################
# https://www.sqlalchemy.org/library.html#pythonsqlalchemytutorial
# https://www.sqlalchemy.org/library.html#zetcodetutorial
import sqlalchemy

from utils import print_table

################  Video-Tutorials  ################
# https://www.youtube.com/watch?v=1Va493SMTcY&ab_channel=SixFeetUp

############## Engine ############################
# create an engine: Initialize with DB-Url
# Docs: https://docs.sqlalchemy.org/en/14/tutorial/engine.html
# <sql_dialect>://<username>:<password>@<host>:<port>/<db_name>
# echo: log sql statements
# engine = sqlalchemy.create_engine("sqlite:///test.db", echo=True, future=True)
# In-memory-Database
engine = sqlalchemy.create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)

############# TRANSACTION ##########################
# Docs: https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html
# get a SqlAlchemy-Proxy to a connection from the engine's connection pool:
con = engine.connect()
con.execute(sqlalchemy.text("Create table person(id integer primary key autoincrement, name text, age integer)"))
con.execute(sqlalchemy.text("Insert into person (name) values (:name)"), {"name": "Maxine"})

# Explicit con.commit() is required here to commit
# auto-begin() transaction
# otherwise changes will not be reflected in DB
con.commit()

# String as Sql-Statements need to get wrapped into a SqlAlchemy-Text-Query:
# From the Docs: textual SQL in day-to-day SQLAlchemy use is by far the exception rather
# than the rule for most tasks, even though it always remains fully available.
stmt = sqlalchemy.text("Select * from person where name=:name")

# read from connection:
res = con.execute(stmt, {"name": "Maxine"})
row = res.fetchone()
# res.fetchall() for multiple rows
print(f"row: {row}")

# Row is a tuple, dict, object of a class

# Access column data of a row by:
# (1) object-property: row.property
# (2a) tuple-index: row[index]
# (2b) tuple destructuring: id, name, age = row
# (3) dict-key: row["key"]
print(f"row: {row}, id: {row.id}, name: {row[1]}, age: {row['age']}")

# Returns connection to the engine's connection pool:
con.close()

# Use Python's Context-Manager for resources:
print("*********** DML: con.connect + con.commit(): No Rollback  **************")
with engine.connect() as con:
    # The transaction is not committed automatically; when we want to commit data we normally
    # need to call Connection.commit() as we’ll see in the next section
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Wolverine", "age": 65})
    con.commit()
print_table(engine, "person")

print("******** DML: con.connect() w/o con.commit(): Rollback expected  *************** ")
with engine.connect() as con:
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Cyclops", "age": 33})
print_table(engine, "person")

print("******** DML: con.begin() with implicit con.commit(): No Rollback  *************** ")
with engine.begin() as con:
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Professor X", "age": 83})
# without connection.commit()
# Transaction will get rollbacked
print_table(engine, "person")

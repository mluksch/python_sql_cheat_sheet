################  Video-Tutorials  ################
# https://www.youtube.com/watch?v=1Va493SMTcY&ab_channel=SixFeetUp

################  Read-Tutorials  ################
# https://www.sqlalchemy.org/library.html#pythonsqlalchemytutorial
# https://www.sqlalchemy.org/library.html#zetcodetutorial
import sqlalchemy

# create an engine:
# Initialize with DB-Url:
# <engine_type>://<username>:<password>@<host>:<port>/<db_name>
engine = sqlalchemy.create_engine("sqlite:///test.db")

# get a SqlAlchemy-Proxy to a connection from the engine's connection pool:
con = engine.connect()

# String as Sql-Statements need to get wrapped into a SqlAlchemy-Text-Query:
print("******** Select stmt ********")
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
print("******** update with commit ********")
with engine.connect() as con:
    # will close connection and commit everything
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Maximus", "age": 65})

with engine.connect() as con:
    # will close connection and commit everything
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Maximus", "age": 65})

print("******** Read ********")
with engine.connect() as con:
    res = con.execute(sqlalchemy.text("Select * from person"))
    rows = res.fetchall()
    print(f"rows: {rows}")

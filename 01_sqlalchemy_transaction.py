################  Read-Tutorials  ################
# https://www.sqlalchemy.org/library.html#pythonsqlalchemytutorial
# https://www.sqlalchemy.org/library.html#zetcodetutorial
import sqlalchemy

from utils import print_table

# SQLAlchemy is a wrapper/abstraction for unifying underlying DBAPIs
# Ideally you can change underlying DBAPIs w/o
# changing any code when using SqlAlchemy

################  Video-Tutorials  ################
# https://www.youtube.com/watch?v=1Va493SMTcY&ab_channel=SixFeetUp

############## Engine ############################
# create an engine: Initialize with DB-Url
# Docs: https://docs.sqlalchemy.org/en/14/tutorial/engine.html
# <sql_dialect>+<DBAPI-driver>://<username>:<password>@<host>:<port>/<db_name>
# echo: log sql statements
# engine = sqlalchemy.create_engine("sqlite:///test.db", echo=True, future=True)
# In-memory-Database
# "Future" means sqlalchemy version 2
print(
    f"""* Creating Engine by DB-Url-Schema: 
<sql_dialect>+<DBAPI-driver>://<username>:<password>@<host>:<port>/<db_name>
""")
engine = sqlalchemy.create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)

############# TRANSACTION ##########################
# Docs: https://docs.sqlalchemy.org/en/14/tutorial/dbapi_transactions.html
# get a SqlAlchemy-Proxy to a connection from the engine's connection pool:
con = engine.connect()

# No cursors like in DBAPIs anymore in SQLAlchemy
# Execute Statements directly on the Connection:
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

# A returned Row is a "Named Tuple" in Python:
# https://stackoverflow.com/questions/2970608/what-are-named-tuples-in-python
# Named tuple instances can be referenced using:
# - object-like variable dereferencing (object.property) or
# - standard tuple syntax (tuple[1])
from collections import namedtuple

TuplePerson = namedtuple('TuplePerson', ['name', 'age'])
hugo = TuplePerson("Hugo", 41)
boss = TuplePerson("Boss", 15)
for tupled in [hugo, boss]:
    print(f"TuplePerson {tupled.name} is {tupled[1]} years old")

##### USE CASES for NAMED TUPLES #######
# (1) you should use named tuples instead of tuples anywhere
# you think object notation will make your code more
# pythonic and more easily readable
# (2) you can also replace ordinary
# immutable classes that have no functions, only fields with them
# (3) However, as with tuples, attributes in named tuples are immutable:

# Access column data of a row by:
# (1) object-property: row.property
# !!! Will get deprecated in SQL(2a) tuple-index: row[index] !!!
# (2b) tuple destructuring: id, name, age = row
# (3) dict-key: row["key"]
print(f"row: {row}, id: {row.id}, name: {row[1]}, age: {row['age']}")

# SqlAlchemy Row results are Named Tuples!

# Edge cases: assuming you get a row ("Sandy", 44) from Person-Table
# you can do:
# "sandy" in row will return True
# because row is a NamedTuple. But:
# "name" in row will return False
# because row is NOT a Dictionary anymore in SqlAlchemy Version>2.0


# Returns connection to the engine's connection pool:
con.close()

# Use Python's Context-Manager for resources:
print("**** DML: con.connect + con.commit(): Explicit Commit required otherwise implicit Rollback  **************")
with engine.connect() as con:
    # The transaction is not committed automatically; when we want to commit data we normally
    # need to call Connection.commit() as we’ll see in the next section
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Wolverine", "age": 65})
    con.commit()
print_table(engine, "person")

print("**** DML: con.connect() w/o con.commit(): Implicit Rollback if con.commit() is missing  *************** ")
with engine.connect() as con:
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Cyclops", "age": 33})
print_table(engine, "person")

print("**** DML: con.begin() with implicit con.commit(): No implicit Rollback *************** ")
with engine.begin() as con:
    res = con.execute(sqlalchemy.text("insert into person(name, age) values (:name, :age)"),
                      {"name": "Professor X", "age": 83})
# without connection.commit()
# Transaction will get rollbacked
print_table(engine, "person")

# For Read-Operations you do not need Transaction of course:
with engine.connect() as con:
    res = con.execute(sqlalchemy.text("Select * from person"))
    # Row-Operations in SqlAlchemy 2.x:
    # scalars: Select columns on execution results
    # all: returns a list similar to fetchall()
    names = res.scalars("name").all()
    print(f"All names in Person-Table: {', '.join(names)}")

# close() on connection will release connection
# to the connection pool of the engine
con = engine.connect()
con.close()

# but normally explicitly calling con.close() is not required
# because connection is created within a context:
with engine.connect() as con:
    pass
    # implicitly con.close() when leaving the block here

###### How use Connections & Engine ################
# Engine should be global in SQLAlchemy
# Connection should be local, i.e.:
# each time created
# (for example: for insert/update/deletes
# create a connection for a transaction and
# close connection afterwards


### Difference between:
# (1) connect()
print("******* engine.connect() has no implicit auto-commit() ******")
with engine.connect() as con:
    con.execute(sqlalchemy.text("Insert into person (name, age) values (:name, :age)"), {
        "name": "Rogue",
        "age": 22
    })
    # con.commit()
    # will not auto-commit, when leaving scope
    # connection will just get closed()

# (2) begin()
print("****** engine.begin() has implicit auto-commit() *****")
with engine.begin() as con:
    con.execute(sqlalchemy.text("Insert into person (name, age) values (:name, :age)"), {
        "name": "Magneto",
        "age": 22
    })
    # will auto-commit, when leaving scope
    # connection will just get closed()

print_table(engine, "person")

# a connection can be used to begin a transaction:
with engine.connect() as con:
    with con.begin() as transaction:
        # placeholder for DB-Mutations:
        pass
        # Rollback DB-Mutations &
        # Close Transaction
        transaction.rollback()
        # placeholder for DB-Mutations:
    with con.begin() as transaction:
        # placeholder for DB-Mutations:
        pass
        # Explicit Commit is not required here but
        # explicit Commit will close Transaction
        transaction.commit()
    with con.begin() as transaction:
        # placeholder for DB-Mutations:
        pass
        # any raised exception will
        # also rollback DB-Mutations &
        # Close Transaction
        raise Exception()

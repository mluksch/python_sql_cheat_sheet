# psycopg2 lib needs to get installed
# Sounds like: Psycho-Pg
import os

import psycopg2

# psycopg3 is in the works with asyncio:
# https://www.psycopg.org/psycopg3/docs/basic/install.html

# create a connection
# transaction is started by default:
con = psycopg2.connect(os.getenv("POSTGRES_URL"))

# create a cursor for executing sql stmts:
cursor = con.cursor()

# create table:
# serial is an "integer autoincrement" in postgres
cursor.execute("Create table if not exists pets(id serial primary key, name text, species text, age integer)")

# Delete rows:
cursor.execute("Delete from pets")

# Explicit con.commit() is required here to commit
# auto-begin() transaction
# otherwise changes will not be reflected in DB
con.commit()

print("****** Insert Rows *****************")
# Insert rows: Tuple Placeholder Style with "%s"
pets = [("Max", "dog", 6), ("Sam", "cat", 11), ("Jerome", "dog", 22), ("Yuna", "cat", 13), ("Vlad", "cat", 4)]
cursor.executemany("Insert into pets(name, species, age) values (%s,%s,%s)", pets)

print("****** Select *********************")
# Read data:
cursor.execute("Select * from pets where species=%s", ("cat",))
results = cursor.fetchall()
for id, name, species, age in results:
    print(f"({id}) Species: {species},  Name: {name}, age: {age}")

print("****** update **************************")
# update data: Named Placeholder Style with "%(key)s"
for id, name, species, age in results:
    cursor.execute("update pets set age=%(age)s where id=%(id)s", {"id": id, "age": age + 100})

cursor.execute("Select * from pets")
results = cursor.fetchall()
print("******** select **************************")
for id, name, species, age in results:
    print(f"({id}) Species: {species},  Name: {name}, age: {age}")

# Explicit con.commit() is required here to commit
# auto-begin() transaction
con.commit()
con.close()

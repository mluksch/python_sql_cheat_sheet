################  Video-Tutorials  ################
# https://www.youtube.com/watch?v=Ohj-CqALrwk&list=PLXoQruUZzEmHEq9462MgL3mPLeQ0VULeQ&index=5&t=500s
# https://www.youtube.com/watch?v=MkGQmZoMuRM&list=PLXoQruUZzEmHEq9462MgL3mPLeQ0VULeQ&index=5&ab_channel=PythonSimplified

################  Read-Tutorials  ################
# https://sqlite.org/cli.html

################  CLI: sqlite3 test.db  ################
# create table persons(id integer primary key autoincrement, name text);
# .tables               # display all tables
# .schema persons       # display schema of table
# .help                 # show help

## csv export:
# .mode csv             # set output mode
# .headers on           # display column headers
# .output FILE          # send output to file

# sqlite3 lib is by default included
import sqlite3

# create a connection
# transaction is started by default:
con = sqlite3.connect("test.db")
# Most DBAPIs have a transaction ongoing which is auto-begin() on connect()
# which will begin a transaction implicitly

# create a cursor for executing sql stmts:
cursor = con.cursor()

# create table:
cursor.execute("Create table if not exists person(id integer primary key autoincrement, name text, age integer)")

# Delete rows:
cursor.execute("Delete from person")
con.commit()

print("****** Insert Rows *****************")
# Insert rows: Tuple Placeholder Style with "?"
person_data = [("Max", 66), ("Sam", 24), ("Jerome", 33)]
cursor.executemany("Insert into person(name, age) values (?,?)", person_data)

print("****** Select *********************")
# Read data:
cursor.execute("Select * from person where age > 30")
results = cursor.fetchall()
for id, name, age in results:
    print(f"({id}) Name: {name} is {age} years old.")

print("****** update **************************")
# update data: Named Placeholder Style with :key
cursor.execute("update person set age=:age, name=:new_name where name=:old_name",
               {"old_name": "Max", "age": 19, "new_name": "Maxine"})

cursor.execute("Select * from person")
results = cursor.fetchall()
print("******** select **************************")
for id, name, age in results:
    print(f"({id}) Name: {name} is {age} years old.")

# commit transaction:
con.commit()
con.close()

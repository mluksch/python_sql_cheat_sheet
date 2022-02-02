# ORM
# Docs:
# https://docs.sqlalchemy.org/en/14/orm/session_basics.html#basics-of-using-a-session

# The ORM typically does not use the Engine directly
# Proxied by a Session-Object in order
# to have the Session-Object decide
# when to talk to the Database and not the User anymore.
# (By default lazily evaluation of CRUD-operations)


import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine(os.getenv("POSTGRES_URL_SQL_ALCHEMY"))

# Best practice:
# Create a Sessionmaker as a Factory for Session-Objects
# & put it into global scope as Central control point
# for creating Session-Object throughout your application
# Reason: https://docs.sqlalchemy.org/en/14/orm/session_basics.html#when-do-i-make-a-sessionmaker
Session = sessionmaker(engine)

# configure later:
Session.configure(bind=os.getenv("POSTGRES_URL_SQL_ALCHEMY"))

# Workflow:
# Create a Session-Object by a global Sessionmaker:
with Session() as session:
    pass

# - Translates Tables to Custom-Class-Object & vice versa
# - Writing SQL-Statements as Custom-Class-Object
# - Versioning of Objects for concurrent updates#
# - Custom-Class-Object-Inheritance


# Create Base Class
import sqlalchemy

Base = sqlalchemy.ext.de

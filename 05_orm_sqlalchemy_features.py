# ORM
# Docs:
# https://docs.sqlalchemy.org/en/14/orm/session_basics.html#basics-of-using-a-session

# The ORM typically does not use the Engine directly
# Proxied by a Session-Object in order
# to have the Session-Object decide
# when to talk to the Database and not the User anymore.
# (By default lazy evaluation of CRUD-operations)
# In ORM a Session-Object will handle the job of connecting,
# committing, and releasing connections to this engine.
# Session = Factory for DB-Connections

import os

import sqlalchemy as sa
import sqlalchemy.orm as orm

engine = sa.create_engine(os.getenv("POSTGRES_URL_SQL_ALCHEMY"))

# Best practice:
# (1) Create a Sessionmaker as a Factory for Session-Objects
# & put it into global scope as Central control point
# for creating Session-Object throughout your application
# Reason: https://docs.sqlalchemy.org/en/14/orm/session_basics.html#when-do-i-make-a-sessionmaker
Session = orm.sessionmaker(engine)

# Why indirect access to Session with a Sessionmaker?
# A Session is a stateful object for use in a single Thread
# But not made for simultaneous access such as in a web app

# possible to configure later:
# Session.configure(bind=os.getenv("POSTGRES_URL_SQL_ALCHEMY"))

# (2) Workflow:
# Get a Session-Object from the global Sessionmaker-Factory-Object:
with Session() as session:
    # Do stuff
    # session.add(some_object)
    # session.add(some_other_object)
    # session.commit()
    pass

# Just syntactic sugar for
# rollback in case of exceptions:
# with Session(engine) as session:
#     session.begin()
#     try:
#         session.add(some_object)
#         session.add(some_other_object)
#     except:
#         session.rollback()
#         raise
#     else:
#         session.commit()


# - Translates Tables to Custom-Class-Object & vice versa
# - Writing SQL-Statements as Custom-Class-Object
# - Versioning of Objects for concurrent updates#
# - Custom-Class-Object-Inheritance

# Registry for all Mappings: Class / Tables
# Plus Metadata of Tables
mapper_registry = orm.registry()


# You can also inherit from a Base class:
# import sqlalchemy.ext.declarative as ext
# Base = ext.declarative_base()
# class User(Base):
#    pass

# Declarative Mapping of Class/Table
# for ORM by Class Definitions:
@mapper_registry.mapped
class User:
    # Table Value in mapper_registry:
    __tablename__ = "user"

    # all Columns in Table as static class fields:
    id = sa.Column(sa.Integer, primary_key=True)
    first_name = sa.Column(sa.String)
    last_name = sa.Column(sa.String)
    age = sa.Column(sa.Integer)

    # define a string representation
    # for logging objects
    def __repr__(self):
        return f"User({self.id}, {self.first_name}, {self.last_name}, {self.age})"


# We can create Tables from registry's metadata:


with engine.begin() as connection:
    # mapper_registry.metadata.drop_all()
    mapper_registry.metadata.create_all(connection)
    # import csv files: ...

with Session() as session:
    spongebob = User(first_name="Spongebob", last_name="Squarepants", age=99)
    session.add(spongebob)
    session.commit()

with Session() as session:
    # user is a named tuple:
    user = session.execute(sa.select(User).filter_by(first_name="Spongebob")).fetchone()
    # one() vs fetchone()
    # result.one() will throw an Error,
    # if multiple have been found
    # result.fetchone() will
    # return first result of possibly many
    print(f"user: {user}")

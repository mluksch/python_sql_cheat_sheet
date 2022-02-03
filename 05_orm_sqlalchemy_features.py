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

import sqlalchemy as sa
import sqlalchemy.orm as orm

import utils

engine = sa.create_engine("sqlite:///:memory:")

# Best practice:
# (1) Create Engine & Sessionmaker:
#       engine = create_engine(<db_url>)
#       Session = sessionmaker(engine)
# (2.a) Create Registry:
#       mapper_registry = registry() or create Base-class:
#       Base = declarative_base()
# (2.b) Class Definition for Tables:
#       @mapper_registry.mapped annotated class or inherited from Base with __tablename__
# (2.c) Create Tables from Registry:
#       mapper_registry.metadata.create_all(connection) or
#       Base.metadata.create_all(connection)
# (3) Workflow:
# with Session() as session:
#       Create + Updates + Deletes: Add objects to session
#       session.add(...)
#       Queries: Get objects from session
#       session.execute(sa.select(User).filter_by(first_name="Spongebob2")).fetchone()


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

# Workflow:
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

# (2.a) Registry for all Mappings: Class / Tables
# Plus Metadata of Tables
mapper_registry = orm.registry()


# You can also inherit from a Base class:
# import sqlalchemy.ext.declarative as ext
# Base = ext.declarative_base()
# class User(Base):
#    pass

# (2.b) Declarative Mapping of Class/Table
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

    # foreign key column stored at address-Table
    addresses = orm.relationship(
        # Table-Class:
        "Address",
        # Class-Field in the other Table-Class:
        back_populates="user")

    # define a string representation
    # for logging objects
    def __repr__(self):
        return f"User({self.id}, {self.first_name}, {self.last_name}, {self.age})"


# Joining tables in ORM:
@mapper_registry.mapped
class Address:
    __tablename__ = "address"
    id = sa.Column(sa.Integer(), primary_key=True)
    user_id = sa.Column("user_id", sa.ForeignKey("user.id"))
    user = orm.relationship(
        # Table-Class:
        "User",
        # Field in Table-Class:
        back_populates="addresses")
    street = sa.Column(sa.String())

    def __repr__(self):
        return f"Address(id: {self.id}, user_id: {self.user_id}, street: {self.street})"


# (2.c) Create Tables
# We can create Tables from registry's metadata:
with engine.begin() as connection:
    # mapper_registry.metadata.drop_all()
    mapper_registry.metadata.create_all(connection)
    # import csv files: ...

# (3) Workflow
# Create:
utils.print_table(engine, "user")
with Session() as session:
    spongebob = User(first_name="Spongebob", last_name="Squarepants", age=99)
    patrick = User(first_name="Patrick", last_name="Starfish", age=99, addresses=[Address(street="Starfish Alley 99")])
    mr_crabs = User(first_name="Mr.", last_name="Crab", age=99)
    # add single item
    session.add(spongebob)
    # add multiple items
    session.add_all([
        patrick,
        mr_crabs
    ])
    # Send to Database:
    session.commit()

utils.print_table(engine, "user")

# Read
with Session() as session:
    # !!! "session.execute" returns Named-Tuples but not User-ORM-managed objects !!!
    # Returns either None or first element
    user_fetchone = session.execute(sa.select(User).filter_by(first_name="Spongebob2")).fetchone()
    # first() same as fetchone():
    user_first = session.execute(sa.select(User).filter_by(first_name="Spongebob")).first()
    try:
        # Returns exactly 1 element or raise Excpetion:
        user_one = session.execute(sa.select(User).filter_by(first_name="Spongebob2")).one()
    except Exception as e:
        print(f"session.execute(...).one(): {e}")
        user_one = e
    #####   Difference between: one() vs fetchone()/first()   #######
    # result.one() will throw an Error, if multiple results or no results are found
    # result.fetchone() will return first result of possibly many or None
    print(f"user_fetchone: {user_fetchone}")
    print(f"user_first: {user_first}")
    print(f"user_one: {user_one}")
    # "session.query" returns ORM-managed-Objects which we can be updated:
    spongebob = session.query(User).filter_by(first_name="Spongebob").first()
    # if there is no foreign key, we get an empty list:
    print(f"** spongebob.addresses : {spongebob.addresses}")
    # Adding objects to 1-to-many-relationship automatically sets all ids, foreign_keys:
    spongebob.addresses.append(Address(street="Spongy Road 123"))
    spongebob2 = session.query(User).filter(User.first_name.in_(["Spongebob"])).first()
    # no need to add updated object to session once more:
    spongebob.first_name = "Spambot"
    spongebob2.last_name = "Circlepants"
    session.commit()

utils.print_table(engine, "user")
utils.print_table(engine, "address")

# After a commit or rollback, Session invalidates all of its data.
# If you access objects from the session now,
# a new transaction will get implicitly created and a query is executed.
# So there is always a transaction in place by default, even for reads.
# => High Isolation of Transaction

# Querying in ORM:
# All defined class fields act like Column objects producing SQL expresion
expression = User.first_name.in_(["test"])
print(f"Expression: {expression}")

# Difference between namedtuples and ORM-managed objects:
with Session() as session:
    # ORM-managed object:
    patrick = session.query(User).filter(User.first_name.in_(["Patrick"])).first()
    print(f"session.query(User) - Patrick ORM based object: {patrick}")
    print(f"Patrick.addresses: {patrick.addresses}")
    # named tuple:
    patrick = session.query(User.first_name, User.age).filter(User.first_name.in_(["Patrick"])).first()
    print(f"session.query(User.first_name, User.age) - Patrick named tuple: {patrick}")

# Difference between filter(...) and filter_by(...)
# filter accepts *args aka positional arguments
# filter_by accepts **kwargs aka named arguments

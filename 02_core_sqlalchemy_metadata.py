# CORE #
# not needed

import pprint

import pandas as pd
import sqlalchemy

import utils

########################## METADATA ##########################
# (1) contains information about table structures
# (2) can generate the Schema of all Tables
#     i.e. used to create Tables or vice versa:
#     From an existing Database or Schema the Metadata
#     can get generated
# (3) used for Sql generation and ORMapping in SqlAlchemy
# (4) mainly serves as a Collection of Tables
# (5) can be read by Migration Tools (Sql Alembic)

# (I) Adding Table-Objects to Metadata:
print("(I) Adding Table-Objects to Metadata")
metadata = sqlalchemy.MetaData()
species_table = sqlalchemy.Table(
    "species",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String(100), primary_key=True),
    sqlalchemy.Column("description", sqlalchemy.Text),
)
print(f"* Table-Object species with columns: {species_table.c}")

animal_table = sqlalchemy.Table(
    "animal",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("gender", sqlalchemy.Enum("male", "female"), nullable=False),
    sqlalchemy.Column("name", sqlalchemy.String(250), nullable=False),
    sqlalchemy.Column("species", sqlalchemy.ForeignKey("species.id"), nullable=False),
    sqlalchemy.Column("birthday", sqlalchemy.DateTime),
    sqlalchemy.Column("price", sqlalchemy.Numeric(10, 2)),
)
print(f"* Table-Object animal with columns: {species_table.c}")

# Metadata contains now all Table informations:
# Dictionary of Tables
print(f"* Table-Objects stored in Metadata: {', '.join(metadata.tables.keys())}")

# (II) Create all Tables from Metadata:
print("(II) Create all Tables from Metadata")
print(f"* Create Tables from Metadata: metadata.create_all(connection)")
engine = sqlalchemy.create_engine("sqlite:///:memory:", echo=False, future=True)
with engine.begin() as con:
    # creates all tables from metadata:
    metadata.create_all(con)
    # import some test data:
    df_species = pd.read_csv("metadata_species.csv")
    df_animal = pd.read_csv("metadata_animal.csv")
    df_species.to_sql("species", con=con,
                      # 'replace' for dropping table, if exists
                      # 'append' otherwise
                      if_exists='append',
                      # ignore DataFrame-index:
                      index=False)
    df_animal.to_sql("animal", con=con,
                     # 'replace' for dropping table, if exists
                     # 'append' otherwise
                     if_exists='append',
                     # ignore DataFrame-index:
                     index=False)

utils.print_table(engine, "species")
utils.print_table(engine, "animal")

# (III) Reflection: Create Metadata from existing Tables in a DB
print("(III) Reflection: Create Metadata from existing Tables in a DB")
metadata2 = sqlalchemy.MetaData()
with engine.connect() as con:
    # add existing tables in the Database to metadata-collection:
    print(f"sqlalchemy.Table(\"animal\", metadata2, autoload_with=con)")
    animal_table2 = sqlalchemy.Table("animal", metadata2, autoload_with=con)
    species_table2 = sqlalchemy.Table("species", metadata2, autoload_with=con)

# Utility: Using Inspector to get table infos from existing Tables in a DB
inspector = sqlalchemy.inspect(engine)
# columns_info = inspector.get_columns("<table_name>")
col_infos = inspector.get_columns("animal")
print(
    f"""
* Inspector for extracting Table infos from DB: 
inspector.get_columns(\"animal\") = {pprint.pformat(col_infos)[0:100] + '...'}
""")

# Getting Table information from Metadata:
table_info = metadata2.tables["species"]
print(f"""
* Table infos in Metadata: 
metadata2.tables[\"species\"] = {pprint.pformat(metadata.tables['species'])[0:100] + '...'}
""")

# When is Reflection used?
# When you have an existing DB and for Migration

# (IV) Mapping SqlAlchemy-Types to SQL-Types
table_visitors = sqlalchemy.Table(
    "visitor",
    metadata,
    # Columns with different SqlAlchemy-DataTypes
    sqlalchemy.Column(
        "name",
        # Maps to SQL-Type: VARCHAR
        sqlalchemy.String(),
        primary_key=True),
    sqlalchemy.Column("age",
                      # Maps to SQL-Type: INT
                      sqlalchemy.Integer()),
    sqlalchemy.Column("interview",
                      # Rather use String! Maps to SQL-Type: VARCHAR, NVARCHAR
                      sqlalchemy.Unicode()),
    sqlalchemy.Column("is_adult",
                      # Maps to SQL-Type: BOOLEAN (not supported by every DBs), INT, BIT
                      sqlalchemy.Boolean()
                      ),
    sqlalchemy.Column("job",
                      # Maps to SQL-Type: VARCHAR (most DB doesnt have an Enum-Type)
                      sqlalchemy.Enum(
                          "SOFTWARE_DEVELOPER", "IT_ARCHITECT", "PROJECT_MANAGER",
                          # !! For Postgres "name" is required for Enums:
                          name="job_type"
                      )),
    sqlalchemy.Column("visit_date",
                      # Maps to SQL-Type: TIMESTAMP, Maps to Python Datetime
                      sqlalchemy.DateTime()),
    sqlalchemy.Column("account_balance",
                      # Maps to SQL-Type: BIGINT
                      sqlalchemy.BigInteger()),
    sqlalchemy.Column("account_balance",
                      # Maps to SQL-Type: DATE, Maps to Python: Date
                      sqlalchemy.Date()),
    sqlalchemy.Column("profile_file",
                      # Maps to SQL-Type: BLOB
                      sqlalchemy.BLOB),
    sqlalchemy.Column("weight",
                      # Maps to SQL-Type: precision numerics, Maps to Python: Decimal()
                      sqlalchemy.DECIMAL),
    sqlalchemy.Column("height",
                      # Maps to SQL-Type: FLOAT, Maps to Python: float
                      sqlalchemy.Float()),
    sqlalchemy.Column("json_dump",
                      # Maps to SQL-Type: JSON. Supported by Postgresql, MySql, SQLite
                      sqlalchemy.JSON())
)

with engine.begin() as con:
    # Delete single Tables:
    print(f"* Delete single Tables: species_table.drop(con)")
    # checkFirst=True (default) will check, if table exists and abort
    # checkFirst=False will not check before dropping
    # and throw Exception "no such table", if table does not exist
    species_table.drop(con, checkfirst=False)
    animal_table.drop(con, checkfirst=False)
    # Delete all Tables:
    print(f"* Delete all Tables: metadata.drop_all(con)")
    metadata.drop_all(con, checkfirst=True)
    # Create all Tables:
    print(f"* Create all Tables: metadata.create_all(con)")
    metadata.create_all(con, checkfirst=True)

utils.print_table(engine, 'species')
utils.print_table(engine, 'animal')
utils.print_table(engine, 'visitor')

print(f"* Table visitor Created: {pprint.pformat(metadata.tables['visitor'])[0:100] + '...'}")

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

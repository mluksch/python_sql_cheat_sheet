import sqlmodel
import sqlmodel_db as db
import fastapi

engine = sqlmodel.create_engine("sqlite:///shopsystem.db")

with sqlmodel.Session(engine) as session:
    pass


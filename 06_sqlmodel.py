import sqlmodel
import sqlmodel_db as db

engine: sqlmodel = sqlmodel.create_engine("sqlite:///shopsystem.db")

# create all tables:
sqlmodel.SQLModel.metadata.create_all(engine)

with sqlmodel.Session(engine) as session:
    # Create:

    pass
    # Read:
    pass
    # Update:
    pass
    # Delete:
    pass


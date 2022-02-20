import typing

import sqlmodel


# Declare SQLModel-classes for each Table:
# - Each field requires a type.
# - Some field can get assigned default values: 
# * sqlmodel.Field(...) with metadata: primary_key, foreign_key, index, 
#   sa_column_kwargs={"unique": True} for unique constraints: https://github.com/tiangolo/sqlmodel/issues/82
# * sqlmodel.Relationship with metadata: back_populates="property-in-other-table"
class Shop(sqlmodel.SQLModel, table=True):
    id: typing.Optional[int] = sqlmodel.Field(primary_key=True)
    name: str
    purchases: typing.Optional[typing.List["Purchase"]] = sqlmodel.Relationship(
        back_populates="shop",
        sa_relationship_kwargs={
            # Cascade: orphan-delete
            # if purchases-field gets deleted in Product,
            # then purchases-entity will get deleted in Purchase-Table as well
            "cascade": 'delete-orphan',
        }
    )


# many-to-many association table between Shop & Customer:
class Purchase(sqlmodel.SQLModel, table=True):
    id: typing.Optional[int] = sqlmodel.Field(primary_key=True)
    shop_id: int = sqlmodel.Field(foreign_key="shop.id")
    customer_id: int = sqlmodel.Field(foreign_key="customer.id")
    product_id: int = sqlmodel.Field(foreign_key="product.id")
    shop: Shop = sqlmodel.Relationship(back_populates="purchases")
    customer: "Customer" = sqlmodel.Relationship(
        back_populates="purchases")
    product: "Product" = sqlmodel.Relationship(
        back_populates="purchases")


class Product(sqlmodel.SQLModel, table=True):
    id: typing.Optional[int] = sqlmodel.Field(primary_key=True)
    title: str
    price: float
    purchases: typing.Optional[typing.List[Purchase]] = sqlmodel.Relationship(
        back_populates="product",
        sa_relationship_kwargs={
            # Cascade: orphan-delete
            # if purchases-field gets deleted in Product,
            # then purchases-entity will get deleted in Purchase-Table as well
            "cascade": 'delete-orphan',
        }
    )


class Customer(sqlmodel.SQLModel, table=True):
    id: typing.Optional[int] = sqlmodel.Field(primary_key=True)
    first_name: str
    last_name: str
    age: typing.Optional[int]
    purchases: typing.Optional[typing.List[Purchase]] = sqlmodel.Relationship(
        back_populates="customer",
        sa_relationship_kwargs={
            # Cascade: orphan-delete
            # if purchases-field gets deleted in Customer,
            # then purchases-entity will get deleted in Purchase-Table as well
            "cascade": 'delete-orphan',
        }
    )

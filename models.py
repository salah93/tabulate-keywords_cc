from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine


Base = declarative_base()


class Query(Base):
    __tablename__ = 'query'
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    query = Column(String(250), nullable=False)
    article_id = Column(String(250), nullable=False)


# Create an engine that stores data in the local directory's
# app.db file.
engine = create_engine('sqlite:///app.db')

# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)

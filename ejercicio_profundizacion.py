import os
import csv
import re
import sqlite3

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

engine = sqlalchemy.create_engine("sqlite:///secundaria.db")
base = declarative_base()
session = sessionmaker(bind=engine)()

from config import config

script_path = os.path.dirname(os.path.realpath(__file__))

config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)

class Author(base):
    __tablename__ = "autor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Author: {self.name}"


class Book(base):
    __tablename__ = "libro"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    pags = Column(Integer)
    author_id = Column(Integer, ForeignKey("autor.id"))
    autor = relationship("Author")

    def __repr__(self):
        return f"Book: {self.title}, title {self.title}, pags {self.pags}, autor {self.autor.name}"


def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)

def add_autor(autor):

    Session = sessionmaker(bind=engine)
    session = Session()

    data = Author(name=autor)
    session.add(data)
    session.commit()

def add_data(title, pags, author):
    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Author).filter(Author.name == author)
    cargar = query.first()

    if cargar is None:
        print(f"el libro {title} no existe con este autor: {author}")
        return

    book = Book(title=title, pags=pags, autor=author)
    book.autor = cargar
    session.add(book)
    session.commit()

def fill():

    with open(dataset['author']) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            add_autor(row['autor'])


    with open(dataset['book']) as fi:
        data = list(csv.DictReader(fi))

        for row in data:
            add_data(row['titulo'], int(row['cantidad_paginas']), row['autor'])

def fetch(id):
 
    Session = sessionmaker(bind=engine)
    session = Session()

    if id == 0:
        query = session.query(Book).order_by(Book.title.desc())

        for data in query:
            print(data)

    else:
        book_filter = session.query(Book).filter(Book.id == id)

        for data in book_filter:
           print("Libro filtrado:", data)

def search_author(valor):

    Session = sessionmaker(bind=engine)
    session = Session()

    autor_filter = session.query(Book).join(Book.autor).filter(Book.title == valor)

    for data in autor_filter:
        autor = data.autor
        return autor

if __name__ == "__main__":
  # Crear DB
  create_schema()

  # Completar la DB con el CSV
  fill()

  # Leer filas
  fetch(1)  # Ver todo el contenido de la DB
  fetch(3)  # Ver la fila 3
  #fetch(20)  # Ver la fila 20

  # Buscar autor
  print(search_author('Relato de un naufrago'))
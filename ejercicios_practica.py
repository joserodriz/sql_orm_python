#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de práctica
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import os
import csv

import sqlalchemy
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


# Crear el motor (engine) de la base de datos
engine = sqlalchemy.create_engine("sqlite:///secundaria.db")
base = declarative_base()

from config import config

# Obtener la path de ejecución actual del script
script_path = os.path.dirname(os.path.realpath(__file__))

# Obtener los parámetros del archivo de configuración
config_path_name = os.path.join(script_path, 'config.ini')
dataset = config('dataset', config_path_name)

class Tutor(base):
    __tablename__ = "tutor"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    
    def __repr__(self):
        return f"Tutor: {self.name}"


class Estudiante(base):
    __tablename__ = "estudiante"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    grade = Column(Integer)
    tutor_id = Column(Integer, ForeignKey("tutor.id"))

    tutor = relationship("Tutor")

    def __repr__(self):
        return f"Estudiante: {self.name}, edad {self.age}, grado {self.grade}, tutor {self.tutor.name}"


def create_schema():
    # Borrar todos las tablas existentes en la base de datos
    # Esta linea puede comentarse sino se eliminar los datos
    base.metadata.drop_all(engine)

    # Crear las tablas
    base.metadata.create_all(engine)

def insert_tutor(name):

    Session = sessionmaker(bind=engine)
    session = Session()

    tutor_name = Tutor(name=name)

    session.add(tutor_name)
    session.commit()
    print(tutor_name)

def insert_estudiante(name_ed, age, grade, name_tut):

    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Tutor).filter(Tutor.name == name_tut)
    tutor_nombre = query.first()

    if tutor_nombre is None:

        print(f"Error al crear la persona {name_ed}, no existe tutor: {name_tut}")
        return

    # Crear la persona
    estudiante = Estudiante(name=name_ed, age=age, grade=grade)
    estudiante.tutor = tutor_nombre

    # Agregar la persona a la DB
    session.add(estudiante)
    session.commit()
    print(estudiante)
    


def fill():
    print('Completemos esta tablita!')
    # Llenar la tabla de la secundaria con al munos 2 tutores
    # Cada tutor tiene los campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del tutor (puede ser solo nombre sin apellido)

    with open(dataset['tut']) as file:
        data = list(csv.DictReader(file))

        for row in data:
            insert_tutor(row['tutor_name'])

    # Llenar la tabla de la secundaria con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # tutor --> el tutor de ese estudiante (el objeto creado antes)

    with open(dataset['estud']) as file:
        data = list(csv.DictReader(file))

        for row in data:
            insert_estudiante(row['name'], int(row['age']), int(row['grade']), row['tutor'])

    # No olvidarse que antes de poder crear un estudiante debe haberse
    # primero creado el tutor.


def fetch():
    print('Comprobemos su contenido, ¿qué hay en la tabla?')
    # Crear una query para imprimir en pantalla
    # todos los objetos creaods de la tabla estudiante.
    # Imprimir en pantalla cada objeto que traiga la query
    # Realizar un bucle para imprimir de una fila a la vez

    Session = sessionmaker(bind=engine)
    session = Session()

    # Buscar todas las personas
    query = session.query(Estudiante)

    for estudiante in query:
        print(estudiante)

def search_by_tutor(tutor):
    print('Operación búsqueda!')
    # Esta función recibe como parámetro el nombre de un posible tutor.
    # Crear una query para imprimir en pantalla
    # aquellos estudiantes que tengan asignado dicho tutor.

    # Para poder realizar esta query debe usar join, ya que
    # deberá crear la query para la tabla estudiante pero
    # buscar por la propiedad de tutor.name

    Session = sessionmaker(bind=engine)
    session = Session()

    # Buscar todas las personas
    lista_busqueda = session.query(Estudiante).join(Estudiante.tutor).filter(Tutor.name == tutor)
    cont_busqueda = session.query(Estudiante).join(Estudiante.tutor).filter(Tutor.name == tutor).count()

    if cont_busqueda == 0:
        print('Tutor: {} no esta en la lista'.format(tutor))
        return
        
    for estudiante in lista_busqueda:
        print(estudiante)

def modify(id, name):
    print('Modificando la tabla')
    # Deberá actualizar el tutor de un estudiante, cambiarlo para eso debe
    # 1) buscar con una query el tutor por "tutor.name" usando name
    # pasado como parámetro y obtener el objeto del tutor
    # 2) buscar con una query el estudiante por "estudiante.id" usando
    # el id pasado como parámetro
    # 3) actualizar el objeto de tutor del estudiante con el obtenido
    # en el punto 1 y actualizar la base de datos

    Session = sessionmaker(bind=engine)
    session = Session()

    query = session.query(Tutor).filter(Tutor.name == name)
    tutor_mod = query.first()

    if tutor_mod is None:
        print('Tutor ingresado no existe')

        return

    query = session.query(Estudiante).filter(Estudiante.id == id)
    estud = query.first()

    if estud is None:
        print('Id de estudiante ingresado no existe')

        return

    estud.tutor = tutor_mod

    session.add(estud)
    session.commit()

    # TIP: En clase se hizo lo mismo para las nacionalidades con
    # en la función update_persona_nationality


def count_grade(grade):
    print('Estudiante por grado')
    # Utilizar la sentencia COUNT para contar cuantos estudiante
    # se encuentran cursando el grado "grade" pasado como parámetro
    # Imprimir en pantalla el resultado

    Session = sessionmaker(bind=engine)
    session = Session()

    # Buscar todas las personas
    cont_grado = session.query(Estudiante).filter(Estudiante.grade == grade).count()

    if cont_grado == 0:
        print('grado: {} no esta en la lista'.format(grade))
        return
    print('La cantidad de estudiantes en grado {} es {}'.format(grade, cont_grado))

    # TIP: En clase se hizo lo mismo para las nacionalidades con
    # en la función count_persona


if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    create_schema()   # create and reset database (DB)
    fill()
    fetch()

    tutor = input('Ingrese tutor a buscar: ')
    search_by_tutor(tutor)

    nuevo_tutor = input('Ingrese nombre de tutor: ')
    id = int(input('Ingrese ID de estudiante a modificar tutor: '))
    modify(id, nuevo_tutor)

    grade = int(input('Ingrese grado a contar: '))
    count_grade(grade)

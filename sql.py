import pandas as pd 
import pyarrow as pa 
import pyarrow.parquet as pq
import sqlite3
import json
import re
import os


def sql():

    conexion = sqlite3.connect('Base_de_datos.sqlite3')
    cursor = conexion.cursor()

    cursor.execute('DROP TABLE IF EXISTS ciudades')
    cursor.execute('CREATE TABLE ciudades (ciudad_id  INTEGER PRIMARY KEY, nombre TEXT)')

    cursor.execute('DROP TABLE IF EXISTS status_code')
    cursor.execute('CREATE TABLE status_code (status_id INTEGER PRIMARY KEY, code INTEGER)')

    cursor.execute('DROP TABLE IF EXISTS info_clima')
    cursor.execute('CREATE table info_clima(id INTEGER PRIMARY key,distancia REAL, fecha REAL, temperatura REAL, humedad REAL, id_corrida TEXT, ciudad_id INTEGER, status_id INTEGER,FOREIGN key (ciudad_id)  REFERENCES ciudades(ciudad_id),FOREIGN key (status_id)  REFERENCES status_code(status_id))')

    lista = list(lista_ciudades())
    for elemento in lista:
        name = elemento
        cursor.execute('INSERT INTO ciudades(nombre) VALUES (?)', (name,))

    carpeta_proyecto = os.getcwd()
    directorio = os.path.join(carpeta_proyecto, 'Datos')
    archivos = os.listdir(directorio)

    for indice, archivo in enumerate(archivos):
        with open(os.path.join(directorio, archivo)) as archivo_json:
            datos = json.load(archivo_json)

        nombre_texto = re.match(r'([^_]+)_\d+\.json', archivo)
        ciudad_id = lista.index(nombre_texto.group(1)) + 1
        code = datos['codigo_HTTP']

        cursor.execute('INSERT INTO status_code (code) VALUES (?)', (code, ))
        contador = indice + 1

        # tabla info_clima
        if code == 200:
            id_corrida = datos['id_corrida']
            distancia = datos['distancia']
            fecha = datos['fecha']
            temperatura = datos['temperatura']
            humedad = datos['humedad']

            cursor.execute('INSERT INTO info_clima (id_corrida, distancia , fecha , temperatura , humedad ,ciudad_id , status_id ) VALUES (?, ?, ?, ?, ?, ?, ?)',
                           (id_corrida, distancia, fecha, temperatura, humedad, ciudad_id, contador))

    conexion.commit()
    conexion.close()

    return


def lista_ciudades():

    carpeta_proyecto = os.getcwd()
    directorio = os.path.join(carpeta_proyecto, 'Datos')
    archivos = os.listdir(directorio)

    nombres_archivos = set()
    for archivo in archivos:
        nombre_texto = re.match(r'([^_]+)_\d+\.json', archivo)
        if nombre_texto:
            nombres_archivos.add(nombre_texto.group(1))

    return(nombres_archivos)


def resume_parquet():
    conexion = sqlite3.connect('Base_de_datos.sqlite3')
    consulta = '''
		SELECT id_corrida,
			MAX(temperatura) AS temperatura_maxima,
			MIN(temperatura) AS temperatura_minima,
			AVG(temperatura) AS temperatura_promedio,
			MAX(humedad) AS humedad_maxima,
			MIN(humedad) AS humedad_minima,
			AVG(humedad) AS humedad_promedio,
			MAX(fecha) AS ultima_actualizacion
		FROM info_clima
		GROUP BY status_id
	'''
    df = pd.read_sql_query(consulta, conexion)
    df.to_parquet('archivo.parquet')

    return


def main():
	sql()
    resume_parquet()

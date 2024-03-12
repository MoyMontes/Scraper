from bs4 import BeautifulSoup
import requests
import json
import re
import datetime
import os


ciudades = ['ciudad-de-mexico', 'monterrey', 'merida', 'wakanda']


def Nombre_Archivo(ciudad, data, fecha):
    carpeta_proyecto = os.getcwd()
    ruta_completa = os.path.join(carpeta_proyecto, 'Datos')

    if not os.path.exists(ruta_completa):
        os.makedirs(ruta_completa)
    nombre_archivo = ciudad + fecha.strftime('_%H%M%S') + '.json'
    with open(os.path.join(ruta_completa, nombre_archivo), 'w') as archivo_json:
        json.dump(data, archivo_json)

    return nombre_archivo


def Datos(ciudades):
    for ciudad in ciudades:
        url = f'https://www.meteored.mx/{ciudad}/historico'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')
        status_code = response.status_code
        fecha = datetime.datetime.now()

        if status_code == 200:

            Distancia = soup.find(id='dist_cant')
            Actualizacion = soup.find(id='fecha_act_dato')
            Tabla = soup.find(id='tabla_actualizacion')
            r = re.compile(r'\d+')
            r = r.findall(Tabla.text)
            data = {
                'peticion': {'url': url, 'metodo': 'GET'},
                'id_corrida': fecha.strftime('%Y%m%d_%H%M%S'),
                'codigo_HTTP': status_code,
                'distancia': Distancia.text,
                'fecha': Actualizacion.text,
                'temperatura': r[0],
                'humedad': r[3]
            }
            nombre_archivo = Nombre_Archivo(ciudad, data, fecha)
            print(nombre_archivo)
            print(data)
        else:
            data = {
                'peticion': {'url': url, 'metodo': 'GET'},
                'id_corrida': fecha.strftime('%Y%m%d_%H%M%S'),
                'codigo_HTTP': response.status_code, }
            nombre_archivo = Nombre_Archivo(ciudad, data, fecha)
            print(nombre_archivo)
            print(data)


def main():
    Datos(ciudades)

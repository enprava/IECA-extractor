import copy
import os
import sys
import re
import requests
import pandas as pd
import itertools
import numpy as np

import logging

import yaml
from ftfy import fix_encoding

from iecasdmx.funciones import mapear_id_por_dimension, read_yaml, write_yaml

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Jerarquia:
    """Estructura de datos para manejar las jerarquias encontradas dentro
    de las consultas del IECA, es necesario hacer una petición HTTP para expandir la jerarquia
    y traernos los valores para generar las listas de código que usaremos en SDMX.

    Args:
        jerarquia (:class:`Diccionario`): Información resumida de la jerarquia obtenida en una consulta anterior.
        configuracion_global (:class:`Diccionario`): Configuración común a todas la ejecución.
        actividad (:class:`Cadena de Texto`): Nombre de la actividad.
    Attributes:
        id_jerarquia (:class:`Cadena de Texto`): concatenación del alias y el código de la jerarquia.
        metadatos (:class:`Diccionario`): Metainformación de la jerarquia con los siguientes campos clave:

            - url
            - cod (Codificación)
            - des (Descripción)
            - position (En desuso)
            - order (En desuso)
            - alias (Las jerarquias pueden tener distintos datos, usamos estos alias para concretizarlos)
            - levels (En desuso)
        datos (:class:`pandas:pandas.DataFrame`): La jerarquia en un cuadro de datos que posteriormente puede ser
            exportada a .CSV para importarse en SDMX.
        """

    def __init__(self, jerarquia, configuracion_global, actividad, categoria, id_consulta):
        self.configuracion_global = configuracion_global
        self.actividad = actividad
        self.metadatos = jerarquia
        pattern = r"jerarquia/(\d+)\?consultaId"
        match = re.search(pattern, self.metadatos["url"])
        valor = "no_encontrado"
        if match:
            valor = match.group(1)
        if "TEMPORAL" not in self.metadatos["alias"]:
            self.metadatos["alias"] = self.metadatos["alias"][:-2] + "_" + valor + self.metadatos["alias"][-2:]
        self.id_jerarquia = self.metadatos["alias"] + '-' + self.metadatos['cod']
        self.categoria = categoria
        self.logger = logging.getLogger(f'{self.__class__.__name__} [{self.id_jerarquia}]')
        self.id_consulta = id_consulta


        self.nombre = self.metadatos["alias"][2:-2]
        self.nombre = self.nombre if self.metadatos["alias"][-2:] == '_0' else self.nombre + self.metadatos["alias"][-2:]


        index = self.id_jerarquia.find(self.nombre)
        self.nombre_mapa = self.id_jerarquia[index - 2:index + len(self.nombre) + 2] if self.metadatos["alias"][
                                                                                        -2:] == '_0' else self.id_jerarquia[
                                                                                                          index - 2:index + len(
                                                                                                              self.nombre)]

        # mapa de la dimension
        self.datos = self.solicitar_informacion_jerarquia()
        self.datos_originales = []
        self.datos_sdmx = []

        self.logger.info('Extrayendo lista de código')

    def convertir_jerarquia_a_dataframe(self, datos_jerarquia):
        """Transforma el diccionario con los datos de la jerarquia a formato tabular, borrando los valores con Código
        duplicado además de añadir el valor **_Z**.

        Returns:
            datos (:class:`pandas:pandas.DataFrame`): La jerarquia en un cuadro de datos./
         """
        self.logger.info('Transformando Jerarquias')
        data = [datos_jerarquia['data']]
        propiedades_jerarquia = self.configuracion_global['propiedades_jerarquias']

        def recorrer_arbol_recursivamente(datos_jerarquia):
            datos_nivel_actual = [[jerarquia[propiedad] for propiedad in propiedades_jerarquia]
                                  for jerarquia in datos_jerarquia]

            es_ultimo_nivel_rama = np.all(
                [jerarquia['children'] == [] or jerarquia['isLastLevel'] for jerarquia in datos_jerarquia])
            if es_ultimo_nivel_rama:
                return datos_nivel_actual

            return datos_nivel_actual + list(itertools.chain(
                *[recorrer_arbol_recursivamente(jerarquia['children']) for jerarquia in datos_jerarquia]))

        datos_jerarquia = recorrer_arbol_recursivamente(data)

        jerarquia_df = pd.DataFrame(datos_jerarquia, columns=[propiedad.upper() for propiedad in propiedades_jerarquia],
                                    dtype='string')
        jerarquia_df = jerarquia_df.replace(to_replace='null', value='')
        jerarquia_df.drop_duplicates('COD', keep='first', inplace=True)


        self.logger.info('Jerarquia transformada')

        return jerarquia_df

    def guardar_datos(self):
        """Accion que guarda la jerarquia en formato .CSV de dos formas:

                - Con el Còdigo de BADEA (No admitido por nuestro framework de SDMX)
                - Sin el código de BADEA (Admitido por nuestro framework de SDMX)

         """
        self.logger.info('Almacenando datos Jerarquia')

        directorio = os.path.join(self.configuracion_global['directorio_jerarquias'], self.actividad)
        directorio_original = os.path.join(directorio, 'original', self.id_consulta)
        directorio_sdmx = os.path.join(directorio, 'sdmx', self.id_consulta)

        columnas = ['ID', 'COD', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
        datos = copy.deepcopy(self.datos)
        datos.columns = columnas

        if not os.path.exists(directorio_original):
            os.makedirs(directorio_original)

        if not os.path.exists(directorio_sdmx):
            os.makedirs(directorio_sdmx)

        self.guardar_datos_originales(datos, directorio_original)
        self.guardar_datos_sdmx(columnas, directorio_sdmx)

        self.logger.info('Jerarquia Almacenada')

    def guardar_datos_originales(self, datos, directorio_original):
        datos[['COD']] = self.formatear_cod(datos[['COD']])
        mapa_padre = self.mapear_padre_cod(datos[['ID', 'COD']].to_dict('tight')['data'])
        datos = datos.replace({'PARENTCODE': mapa_padre})
        self.datos_originales = datos  # Necesario para guardar_datos_sdmx

        datos.to_csv(f'{os.path.join(directorio_original, self.nombre_mapa)}.csv', sep=';', index=False)

    def guardar_datos_sdmx(self, columnas, directorio_sdmx):
        datos = copy.deepcopy(self.datos_originales)  # Se ha ejecutado antes guardar_datos_originales
        self.datos_sdmx = mapear_id_por_dimension(datos[columnas], self.nombre_mapa,
                                                  self.configuracion_global[
                                                      'directorio_mapas_dimensiones'])
        Z = pd.Series({'ID': '_Z', 'NAME': 'No aplica', 'DESCRIPTION': 'No aplica', 'PARENTCODE': None, 'ORDER': None})
        # Cambiamos label por description
        self.datos_sdmx[['NAME', 'DESCRIPTION']] = self.datos_sdmx[['DESCRIPTION', 'NAME']]
        self.datos_sdmx = pd.concat([self.datos_sdmx, Z.to_frame().T], ignore_index=True)

        self.datos_sdmx.drop_duplicates('ID', inplace=True)
        self.datos_sdmx.to_csv(f'{os.path.join(directorio_sdmx, self.nombre_mapa)}.csv', sep=';', index=False)

    def agregar_datos_jerarquia(self):
        datos_jerarquias = read_yaml(self.configuracion_global['directorio_datos_jerarquias'])
        has_changed = False
        if not datos_jerarquias:
            datos_jerarquias = {}
        directorio = os.path.join(self.configuracion_global['directorio_jerarquias'], self.actividad, 'sdmx',
                                  self.id_consulta, self.nombre_mapa)
        archivo = f'{directorio}.csv'.replace('\\', '/')

        nombre = self.nombre_mapa[2:-2] if self.nombre_mapa[-2:] == '_0' else self.nombre_mapa[2:]

        #nombre = nombre[:-2] if nombre[-2:] == '_0' else nombre
        if nombre not in datos_jerarquias:
            datos_jerarquias[nombre] = {'ID': f'CL_{nombre}', 'agency': self.configuracion_global['nodeId'],
                                        'version': '1.0', 'nombre': {'es': nombre},
                                        'description': {'es': self.metadatos['des']},
                                        'fichero': [archivo]}
            has_changed = True
        else:
            if archivo not in datos_jerarquias[nombre]['fichero']:
                datos_jerarquias[self.nombre]['fichero'].append(f'{directorio}.csv')
                has_changed = True
        if has_changed:
            write_yaml(self.configuracion_global['directorio_datos_jerarquias'], datos_jerarquias)

    def solicitar_informacion_jerarquia(self):
        """Realiza la petición HTTP a la API si la jerarquía no se encuentra en nuestro directorio local,
        automáticamente se convierte la jerarquia a dataframe haciendo uso de
        :attr:`iecasdmx.jerarquia.Jerarquia.convertir_jerarquia_a_dataframe`.

        Returns:
            datos (:class:`pandas:pandas.DataFrame`): La jerarquia en un cuadro de datos.
         """
        directorio_csv = os.path.join(self.configuracion_global['directorio_jerarquias'], self.actividad, 'original',
                                      self.id_consulta, self.nombre_mapa + '.csv')
        datos = None
        try:

            if self.configuracion_global["cache_search"]:
                self.logger.info('Buscando el CSV de la jerarquia en local')
                with open(directorio_csv, 'r', encoding='utf-8') as csv_file:
                    datos = pd.read_csv(csv_file, sep=';', dtype='string', keep_default_na=False)
                    self.logger.info('CSV leido correctamente')
            else:
                self.logger.info('Ignorando caché - iniciando peticion a la API del IECA')
                self.logger.info('Iniciando peticion a la API del IECA')
                datos = self.convertir_jerarquia_a_dataframe(requests.get(self.metadatos['url']).json())
                self.logger.info('Petición API Finalizada')

        except Exception as e:
            self.logger.warning('No se ha encontrado el fichero %s', directorio_csv)
            self.logger.warning('Excepción: %s', e)
            self.logger.info('Iniciando peticion a la API del IECA')
            datos = self.convertir_jerarquia_a_dataframe(requests.get(self.metadatos['url']).json())
            self.logger.info('Petición API Finalizada')

        finally:
            if datos is not None:
                self.logger.info('Datos alcanzados correctamente')
            else:
                self.logger.warning('No hay información disponible')
        return datos

    def añadir_mapa_concepto_codelist(self):
        mapa_conceptos_codelists = read_yaml(self.configuracion_global['directorio_mapa_conceptos_codelists'])
        nombre = self.nombre_mapa[2:]
        nombre = nombre[:-2] if nombre[-2:] == '_0' else nombre


        if not mapa_conceptos_codelists or nombre not in mapa_conceptos_codelists:
            mapa_conceptos_codelists[nombre] = {'tipo': 'dimension', 'nombre_dimension': nombre,
                                                'concept_scheme': {'agency': 'ESC01',
                                                                   'id': 'CS_' + self.categoria,
                                                                   'version': '1.0',
                                                                   'concepto': nombre},
                                                'codelist': {'agency': 'ESC01', 'id': 'CL_' + nombre,
                                                             'version': '1.0'},
                                                'nombre': {'es': fix_encoding(self.metadatos['des'])},
                                                'descripcion': {'es': self.metadatos['des']}}
        write_yaml(self.configuracion_global['directorio_mapa_conceptos_codelists'], mapa_conceptos_codelists)

    def mapear_padre_cod(self, datos):
        res = {'': ''}
        for dato in datos:
            res[dato[0]] = dato[1]
        return res

    def formatear_cod(self, cod):
        return cod.apply(lambda x: x.COD[3:] if len(x.COD) > 3 and x.COD[0] == 'P' and x.COD[2] == '_' else x, axis=1)

import json
import os
import sys

import pandas as pd
import requests

import logging

from iecasdmx.ieca.jerarquia import Jerarquia
from iecasdmx.ieca.datos import Datos
from iecasdmx.funciones import read_json, write_json

import unidecode

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Consulta:
    """Este objeto al inicializarse consultara la API del IECA utilizando :attr:`~.id_consulta`.
    Esta clase se encargará de generar las estructuras de datos y metadatos y de ser su recipiente.
    Las medidas se trataran como parte de una dimensión SDMX llamada **INDICATOR** y se manejaran dentro
    de la clase :class:`iecasdmx.datos.Datos`.


    Args:
        id_consulta (:class:`Cadena de Texto`): ID de la consulta que se va a procesar.
        configuracion_global (:class:`Diccionario`): Configuración común a todas las ejecuciones que se realicen.
        configuracion_actividad (:class:`Diccionario`): Configuración común para toda la actividad.
        actividad (:class:`Cadena de Texto`): Nombre de la actividad.

    Attributes:
        id_consulta (:class:`Cadena de Texto`)
        metadatos (:class:`Diccionario`): Metainformación de la consulta con los siguientes campos clave:

            - id
            - title
            - subtitle
            - activity
            - source
            - periodicity
            - type
            - notes

        jerarquias (:obj:`Lista` de :class:`iecasdmx.jerarquia.Jerarquia`): Jerarquias utilizadas en los datos de
            la consulta
        datos (:class:`iecasdmx.datos.Datos`): Datos proporcionados en la consulta.
    """

    def __init__(self, id_consulta, configuracion_global, configuracion_actividad, mapa_conceptos_codelist, actividad):
        self.url_consulta = id_consulta
        self.id_consulta = id_consulta

        self.configuracion_global = configuracion_global
        self.configuracion_actividad = configuracion_actividad
        self.mapa_conceptos_codelist = mapa_conceptos_codelist

        self.actividad = actividad

        self.logger = logging.getLogger(f'{self.__class__.__name__} [{self.id_consulta}]')
        self.logger.info('Inicializando consulta')

        self.metadatos, \
        jerarquias_sin_procesar, \
        self.medidas, \
        datos_sin_procesar = \
            self.solicitar_informacion_api()

        self.jerarquias = [
            Jerarquia(jerarquia, self.configuracion_global, self.actividad, self.configuracion_actividad['categoria'],
                      self.id_consulta)
            for jerarquia in
            jerarquias_sin_procesar]
        self.datos = Datos(self.id_consulta, self.configuracion_global, self.mapa_conceptos_codelist, self.actividad,
                           self.metadatos['periodicity'],
                           datos_sin_procesar,
                           self.jerarquias, self.medidas)

        self.logger.info('Consulta Finalizada')

    @property
    def id_consulta(self):
        return self._id_consulta

    @id_consulta.setter
    def id_consulta(self, value):
        if not isinstance(value, str):
            value = str(value)
        if len(value) > 8:
            value = value.split('?')[0]
        self._id_consulta = value

    def ejecutar(self):
        """Aplica las funciones configuradas en el fichero de configuración **'actividades.yaml'** bajo
        las claves **acciones_jerarquia** y **acciones_datos*.
        """

        for jerarquia in self.jerarquias:
            jerarquia.guardar_datos()
            jerarquia.añadir_mapa_concepto_codelist()
            jerarquia.agregar_datos_jerarquia()

        self.datos.guardar_datos('original')
        self.datos.extender_mapa_nuevos_terminos()
        self.datos.mapear_valores()
        self.datos.mapear_columnas()
        self.datos.borrar_filas(['', '-', '*', 'se'])
        self.datos.guardar_datos('procesados')

        self.actualiza_medidas()

    def solicitar_informacion_api(self):
        """Utilizando :attr:`~.id_consulta` busca el JSON de la consulta en local, y si no, le manda
        la petición a la API del IECA. Si se ha alcanzado la API, se guarda el JSON para acelerar futuras consultas y
        no sobrecargar el sistema. Hemos de tener esto en cuenta, en caso de que las consultas de la API no sean
        inmutables.


        Returns:
            - metainfo (:class:`Diccionario`)
            - hierarchies (:class:`Diccionario`)
            - measures (:class:`Diccionario`)
            - data (:class:`Diccionario`)

         """

        # La maravillosa API del IECA colapsa con consultas grandes (20MB+ aprox)
        directorio = os.path.join(self.configuracion_global['directorio_json'], self.actividad)
        directorio_json = os.path.join(directorio, self.id_consulta + '.json')
        if not os.path.exists(directorio):
            os.makedirs(directorio)
        respuesta = False
        try:

            if self.configuracion_global["cache_search"]:
                self.logger.info('Buscando el JSON de la consulta en local')
                respuesta = read_json(directorio_json)
                self.logger.info('JSON leido correctamente')
            else:
                self.logger.info('Ignorando caché - iniciando peticion a la API del IECA')
                respuesta = requests.get(
                    f"https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/"
                    f"{self.url_consulta}").json()
                self.logger.info('Petición Finalizada')
                self.logger.info('Guardando JSON')
                with open(directorio_json, 'w', encoding='utf-8') as json_file:
                    json.dump(respuesta, json_file)
                self.logger.info('JSON Guardado')

        except Exception as e:
            self.logger.warning('No se ha encontrado el fichero %s', directorio_json)
            self.logger.warning('Excepción: %s', e)
            self.logger.info('Iniciando peticion a la API del IECA')
            respuesta = requests.get(
                f"https://www.juntadeandalucia.es/institutodeestadisticaycartografia/intranet/admin/rest/v1.0/consulta/"
                f"{self.url_consulta}").json()
            self.logger.info('Petición Finalizada')
            self.logger.info('Guardando JSON')
            write_json(directorio_json, respuesta)
            self.logger.info('JSON Guardado')

        finally:
            if respuesta and respuesta['data']:
                self.logger.info('Datos alcanzados correctamente')
            else:
                self.logger.warning('No hay información disponible')
        return respuesta['metainfo'], \
               respuesta['hierarchies'], \
               respuesta['measures'], \
               respuesta['data'] if respuesta else None

    def actualiza_medidas(self):
        mapa = pd.read_csv(f'{self.configuracion_global["directorio_mapas_dimensiones"]}INDICATOR')
        jerarquia = pd.read_csv(f'{self.configuracion_global["directorio_jerarquias"]}/INDICATOR', sep=';')
        for medida in self.medidas:
            if medida['des'] in self.configuracion_global['medidas_reemplazando_obs_status']:
                continue
            if medida['des'] not in mapa.SOURCE.values:
                target = self.__formatea_id_medida(medida['des'])
                mapa.loc[len(mapa)] = [medida['des'], None, target]
            else:
                target = mapa[mapa['SOURCE'] == medida['des']]['TARGET'].values[0]
            jerarquia.loc[len(jerarquia)] = [target, medida['des'], medida['des'], None]
        mapa.drop_duplicates('SOURCE', inplace=True, ignore_index=True)
        jerarquia.drop_duplicates('ID', inplace=True, ignore_index=True)
        mapa.to_csv(f'{self.configuracion_global["directorio_mapas_dimensiones"]}INDICATOR', index=False)
        jerarquia.to_csv(f'{self.configuracion_global["directorio_jerarquias"]}/INDICATOR', index=False, sep=';')

    def __formatea_id_medida(self, medida):
        trozos = medida.split(' ')
        id_medida = ''
        for trozo in trozos:
            try:
                id_medida = f'{id_medida}_{trozo[0:5].upper()}'
            except:
                id_medida = f'{id_medida}_{trozo.replace("%", "PCT").upper()}'
                id_medida = f'{id_medida}_{trozo.replace("*", "").upper()}'
        return unidecode.unidecode(id_medida[1:])

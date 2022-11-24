import os
import sys

import pandas as pd

import logging
import numpy as np
from iecasdmx.funciones import crear_mapeo_por_defecto

from iecasdmx.funciones import strip_accents

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Datos:
    """Estructura de datos para manejar los datos encontrados dentro
    de las consultas del IECA. El proceso de inicialización de esta estructura
    realiza los siguientes acciones de forma configurable:
        - Convierte de JSON a DataFrame utilizando las medidas y jerarquias para generar las dimensiones/columnas.
        - Desacopla las observaciones en base a las medidas utilizando la dimension **INDICATOR**
        - Añade la dimension **FREQ** de SDMX en base a la periodicidad de la consulta.

    Args:
        id_consulta (:class:`Cadena de Texto`): ID de la consulta que se va a procesar.
        configuracion_global (:class:`Diccionario`): Configuración común a todas las ejecuciones que se realicen.
        actividad (:class:`Cadena de Texto`): Nombre de la actividad.
        periodicidad (:class:`Cadena de Texto`): Periodicidad de las observaciones.
        datos (:class:`Diccionario`): Datos de la consulta.
        jerarquias (:obj:`Lista` de :class:`iecasdmx.jerarquia.Jerarquia`): Jerarquias de la consulta
        medidas (:class:`iecasdmx.consulta.medidas`): Medidas de la consulta
    Attributes:
        datos (:class:`pandas:pandas.DataFrame`): Los datos en un cuadro de datos
        datos_por_observacion (:class:`pandas:pandas.DataFrame`): Los datos desacoplados por medidas en columnas
        datos_por_observacion_extension_disjuntos (:class:`pandas:pandas.DataFrame`): Los datos desacoplados
            pero con todas las columnas necesarias para crear un DSD para toda la actividad.

    """

    def __init__(self, id_consulta, configuracion_global, mapa_conceptos_codelist, actividad, periodicidad, datos,
                 jerarquias,
                 medidas):
        self.id_consulta = id_consulta
        self.configuracion_global = configuracion_global
        self.mapa_conceptos_codelist = mapa_conceptos_codelist

        self.actividad = actividad

        self.periodicidad = periodicidad
        self.jerarquias = jerarquias
        self.medidas = medidas

        self.logger = logging.getLogger(f'{self.__class__.__name__} [{self.id_consulta}]')

        self.logger.info('Procesando las observaciones: %s con periodicidad: %s',
                         self.id_consulta, self.periodicidad)
        self.datos = self.convertir_datos_a_dataframe_sdmx(datos)
        self.datos_por_observacion = self.desacoplar_datos_por_medidas()
        self.datos_por_observacion_extension_disjuntos = None
        insertar_freq(self.datos_por_observacion, self.periodicidad)

        self.logger.info('Finalización procesamiento de las observaciones')

    def convertir_datos_a_dataframe_sdmx(self, datos):
        """Transforma el diccionario con los datos de las observaciones a formato tabular válido para SDMX de
        la siguiente forma:
            1. Recorremos el diccionario accediendo de forma ordenada a las dimensiones correspondientes tomando \
                las jerarquias y las medidas como referencia.
            2. Los datos en el JSON están indexados por 'COD' en lugar de por 'ID' que sería más apropiado, aquí \
                cruzamos la información pertinente para mapear COD -> ID.
            3. Rellenamos la dimension **FREQ** de SDMX en base a la periodicidad de la consulta.

        Args:
            datos (:class:`Diccionario`): Datos de la consulta.

        Returns:
            datos (:class:`pandas:pandas.DataFrame`): Las observaciones en un cuadro de datos con la forma tabular
            original del modelado hecho en BADEA.
        """
        self.logger.info('Transformando los datos JSON a DataFrame')

        columnas_jerarquia = [jerarquia.metadatos['alias'] for jerarquia in self.jerarquias]
        columnas_medida = [medida['des'] for medida in
                           self.medidas]
        columnas = columnas_jerarquia + columnas_medida

        try:
            df = pd.DataFrame(datos, columns=columnas)
        except Exception as e:
            self.logger.error('Consulta sin datos - %s', self.id_consulta)
            raise e
        df.columns = columnas

        df[columnas_jerarquia] = df[columnas_jerarquia].applymap(lambda x: x['cod'][-1], na_action='ignore')

        df[columnas_medida] = df[columnas_medida].applymap(
            lambda x: x['val'] if x['val'] != "" else x['format'], na_action='ignore')

        dimension_temporal = self.configuracion_global['dimensiones_temporales']
        # Parece que ellos no utilizan de forma interna el formato fecha de sql.
        if dimension_temporal in df.columns:
            df[dimension_temporal] = transformar_formato_tiempo_segun_periodicidad(df[dimension_temporal],
                                                                                   self.periodicidad)
        df_mapeado = df.copy()
        # Parche IECA ya que están indexando por cod en lugar de id.
        for jerarquia in self.jerarquias:
            columna = jerarquia.metadatos['alias']

            if columna != dimension_temporal:
                df_mapeado[columna] = df.merge(jerarquia.datos, how='left', left_on=columna, right_on='COD')[
                    'ID'].values
                disconexos = df_mapeado[columna].isna()
                if len(df_mapeado[disconexos]):
                    df_mapeado[columna][disconexos] = df[columna][disconexos].map(lambda x: crear_mapeo_por_defecto(x),
                                                                                  na_action='ignore')

        self.logger.info('Datos Transformados a DataFrame Correctamente')
        return df_mapeado

    def desacoplar_datos_por_medidas(self):
        """El formato tabular proporcionado por la API tiene una dimension para cada medida, nuestro modelado
        SDMX consistirá en crear una dimension **INDICATOR** cuyo valor será la medida en sí.

        Esto quiere decir que nuestro cuadro de datos tendrá menos columnas, pero mayor número de filas
        (Numero de observaciones * Numero de medidas).

        Args:
            datos (:class:`pandas:pandas.DataFrame`): Datos de la consulta en un cuadro de datos.

        Returns:
            datos (:class:`pandas:pandas.DataFrame`): Las observaciones en un cuadro de datos adaptado al estandar SDMX.
        """
        self.logger.info('Desacoplando las Observaciones del DataFrame')

        columnas_jerarquia = [jerarquia.metadatos['alias'] for jerarquia in self.jerarquias]
        columnas = columnas_jerarquia + ['INDICATOR', 'OBS_VALUE']
        df = pd.DataFrame(columns=columnas)
        medidas = [medida['des'] for medida in self.medidas]
        for medida in medidas:
            if medida in self.configuracion_global['medidas_reemplazando_obs_status']:
                medidas.remove(medida)
                self.datos.rename(columns={medida: 'OBS_STATUS'}, inplace=True)
                columnas_jerarquia = columnas_jerarquia + ['OBS_STATUS']

        for medida in medidas:
            if medida not in self.configuracion_global['indicadores_a_borrar']:
                self.logger.info('Desacoplando para la medida: %s', medida)

                columnas = columnas_jerarquia + ['OBS_VALUE', 'INDICATOR']
                columnas_ordenadas = columnas_jerarquia + ['INDICATOR', 'OBS_VALUE']
                valores_medida = self.datos[columnas_jerarquia + [medida]].copy()
                valores_medida.loc[:, 'INDICATOR'] = medida
                valores_medida.columns = columnas
                valores_medida = valores_medida[columnas_ordenadas]
                df = pd.concat([df, valores_medida])
                self.logger.info('Medida Desacoplada: %s', medida)

        self.logger.info('DataFrame Desacoplado')
        return df

    def guardar_datos(self, clase):
        """Accion que guarda la jerarquia en formato .CSV de dos formas bifurcando en directorios a traves del
            argumento clase:

            - Con el Còdigo de BADEA (No admitido por nuestro framework de SDMX)
            - Sin el código de BADEA (Admitido por nuestro framework de SDMX)

            Args:
                clase (:class:`Cadena de Texto`): Nombre para el directorio en el que se almacenaran los datos.
         """
        self.logger.info('Guardando datos %s', clase)
        directorio = os.path.join(self.configuracion_global['directorio_datos'], self.actividad, clase)
        if not os.path.exists(directorio):
            os.makedirs(directorio)

        self.datos_por_observacion.to_csv(os.path.join(directorio, str(self.id_consulta) + '.csv'), sep=';',
                                          index=False)

    def mapear_valores(self):
        """Accion que realiza el mapeo de los valores del cuadro de datos configuradas bajo el parámetro
         :obj:`dimensiones_a_mapear` observaciones utilizando los mapas previamente rellenos  del
         fichero de configuracion global.
         """
        self.logger.info('Mapeando observaciones hacia SDMX')
        columnas_a_mapear = [column for column in self.datos_por_observacion.columns if
                             column not in ['OBS_VALUE', 'FREQ']]
        for columna in columnas_a_mapear:
            self.logger.info('Mapeando: %s', columna)
            directorio_mapa = os.path.join(self.configuracion_global['directorio_mapas_dimensiones'], columna)
            mapa = pd.read_csv(directorio_mapa, dtype='string', keep_default_na=False)
            self.datos_por_observacion[columna] = \
                self.datos_por_observacion.merge(mapa, how='left', left_on=columna, right_on='SOURCE')['TARGET'].values

    def extender_mapa_nuevos_terminos(self):
        """Accion que crea/extiende el mapa para las columnas configuradas facilitando al técnico realizar la
        conversión y su posterior reutilización en distintas actividades.
         """
        self.logger.info('Ampliando mapas de dimensiones con nuevas ocurrencias')
        columnas_plantilla = ['SOURCE', 'COD', 'NAME', 'TARGET']
        columnas_jerarquia_alias = [jerarquia.id_jerarquia for jerarquia in self.jerarquias] + ['INDICATOR']
        columnas_jerarquia_id = [jerarquia.id_jerarquia.split('-')[0] for jerarquia in self.jerarquias] + ['INDICATOR']
        directorio_mapas = self.configuracion_global['directorio_mapas_dimensiones']
        if not os.path.exists(directorio_mapas):
            os.makedirs(directorio_mapas)

        for columna_alias, columna_id in zip(columnas_jerarquia_alias, columnas_jerarquia_id):
            self.logger.info('Mapa de Dimension : %s', columna_alias)
            fichero_mapa_dimension = os.path.join(directorio_mapas, columna_id)

            if os.path.isfile(fichero_mapa_dimension):
                df_mapa = pd.read_csv(fichero_mapa_dimension, dtype='string', keep_default_na=False)
            else:
                df_mapa = pd.DataFrame(columns=columnas_plantilla, dtype='string')
            uniques = np.full([len(self.datos_por_observacion[columna_id].unique()), len(columnas_plantilla)], None)
            uniques[:, 0] = self.datos_por_observacion[columna_id].unique()
            df_auxiliar = pd.DataFrame(uniques, columns=columnas_plantilla, dtype='string')
            df_mapa = pd.concat([df_mapa, df_auxiliar]).drop_duplicates('SOURCE', keep='first')
            if columna_id != 'INDICATOR':
                jerarquia_codigos = pd.read_csv(
                    os.path.join(self.configuracion_global['directorio_jerarquias'], self.actividad, 'original',
                                 columna_alias + '.csv'), sep=';', keep_default_na=False,
                    dtype='string')
                df_mapa['COD'][df_mapa['COD'].isna()] = \
                    df_mapa[df_mapa['COD'].isna()].merge(jerarquia_codigos, how='left', left_on='SOURCE',
                                                         right_on='ID')['COD_y']
                df_mapa['NAME'][df_mapa['NAME'].isna()] = \
                    df_mapa[df_mapa['NAME'].isna()].merge(jerarquia_codigos, how='left', left_on='SOURCE',
                                                          right_on='ID')['NAME_y']
            df_mapa.reset_index(drop=True, inplace=True)
            mapeos_incompletos_indices = df_mapa[df_mapa['TARGET'].isna()].index
            if mapeos_incompletos_indices.any():
                self.logger.warning("Nuevos términos añadidos al mapa: %s",
                                    list(df_mapa['SOURCE'].values[mapeos_incompletos_indices]))
                for indice in mapeos_incompletos_indices:
                    df_mapa['TARGET'][indice] = crear_mapeo_por_defecto(df_mapa['SOURCE'][indice])
            else:
                self.logger.info("Todos los elementos son mapeables")
            df_mapa.to_csv(fichero_mapa_dimension,
                           index=False)

    def extender_con_disjuntos(self, dimensiones):
        """Accion que añade las columnas al dataframe para que todas las dimensiones SDMX de la actividad sean
        contempladas en un mismo DSD, añadiendo el valor **_Z** a estas nuevas columnas.

        Args:
            dimensiones (:obj:`Lista` de :class:`Cadena de Texto`): Lista de dimensiones únicas que se encuentran
                en el conjunto de la actividad sobre la que estamos trabajando.
         """
        self.datos_por_observacion_extension_disjuntos = self.datos_por_observacion.copy()
        disyuncion_dimensiones = [dimension for dimension in dimensiones if
                                  dimension not in self.datos_por_observacion.columns]
        self.datos_por_observacion_extension_disjuntos[disyuncion_dimensiones] = '_Z'

    def borrar_datos_duplicados(self):
        """Accion que borra las filas duplicadas sin tener en cuenta **OBS_VALUE**.
         """
        columnas_sin_obs_value = [column for column in self.datos_por_observacion.columns if column != 'OBS_VALUE']
        datos_sin_duplicados = self.datos_por_observacion.drop_duplicates(subset=columnas_sin_obs_value,
                                                                          keep='last')
        num_duplicados = len(self.datos_por_observacion) - len(datos_sin_duplicados)
        if num_duplicados:
            self.logger.warning('Se han eliminado %s duplicados', num_duplicados)
        self.datos_por_observacion = datos_sin_duplicados

    def borrar_datos_duplicados_OBS_VALUE(self):
        """Accion que borra las filas duplicadas teniendo en cuenta **OBS_VALUE**.
         """

        datos_sin_duplicados = self.datos_por_observacion.drop_duplicates(keep='first')
        num_duplicados = len(self.datos_por_observacion) - len(datos_sin_duplicados)
        if num_duplicados:
            self.logger.warning('Se han eliminado %s duplicados', num_duplicados)
        self.datos_por_observacion = datos_sin_duplicados

    def sumar_datos_duplicados(self):
        """Accion que agrupa los datos duplicados y devuelve los datos agregados de **OBS_VALUE**.
         """
        columnas_sin_obs_value = [column for column in self.datos_por_observacion.columns if column != 'OBS_VALUE']
        self.datos_por_observacion['OBS_VALUE'] = pd.to_numeric(self.datos_por_observacion['OBS_VALUE'])
        self.datos_por_observacion = self.datos_por_observacion.groupby(columnas_sin_obs_value, as_index=False)[
            'OBS_VALUE'].sum()
        if self.datos_por_observacion.empty:
            self.logger.error('DataFrame vacio, comprueba el mapeo')

    def mapear_columnas(self):
        """Accion que realiza el mapeo de las columnas del cuadro de datos configuradas bajo el parámetro
         observaciones utilizando el mapa previamente relleno a través del campo :obj:`mapeo_columnas` del
         fichero de configuracion global. Por último limpiamos las jerarquias de los prefiejos y sufijos.
         """
        mapeo_columnas = self.configuracion_global['mapeo_columnas']
        columnas = self.datos_por_observacion.columns

        columnas = [columna[2:] if columna[:2] == 'D_' else columna for columna in columnas]
        columnas = [columna[:-2] if columna[-2:] == '_0' else columna for columna in columnas]
        columnas = [mapeo_columnas[columna] if columna in mapeo_columnas.keys() else columna for columna in columnas]

        self.datos_por_observacion.columns = columnas

    def borrar_filas(self, dics_columna_valor_a_borrar):
        """Accion que elimina del cuadro de datos las filas que contengan los valores proporcionados.

        Args:
            dics_columna_valor_a_borrar (:obj:`Lista` de :class:`Cadena de Texto`): Lista de diccionarios con cuyo par
                clave-valor es la columna-valor deseado para la eliminación.
         """

        for dic in dics_columna_valor_a_borrar:
            columna = list(dic.keys())[0]
            valor = dic[columna]
            self.datos_por_observacion = self.datos_por_observacion[self.datos_por_observacion[columna] != valor]

    def mostrar_df(self):
        # print('mostrando df',self.datos.to_string())
        pass


def transformar_formato_tiempo_segun_periodicidad(serie, periodicidad):
    """Transforma la dimension temporal de un cuadro de datos para que se adecue al formato de tiempo utilizado
    en SDMX.

    Args:
        serie (:class:`pandas:pandas.Series`): Serie perteneciente al cuadro de datos a transformar
        periodicidad (:class:`Cadena de Texto`): Periodicidad de la consulta.
     """

    dimension_a_transformar = ['Mensual', 'Trimestral', 'Mensual  Fuente: Instituto Nacional de Estadística']
    if periodicidad in dimension_a_transformar:
        serie = serie.apply(lambda x: x[:4] + '-' + x[4:])
    return serie


def insertar_freq(df, periodicidad):
    """Añade los valores a la columna **'FREQ'** dependiendo de un mapa simple de periodicidad.

    Args:
        df (:class:`pandas:pandas.DataFrame`): Cuadro de datos al que añadir la frecuencia.
        periodicidad (:class:`Cadena de Texto`): Periodicidad de la consulta.
     """
    diccionario_periodicidad_sdmx = {'Mensual': 'M', 'Anual': 'A',
                                     'Mensual  Fuente: Instituto Nacional de Estadística': 'M', '': 'M',
                                     'Anual. Datos a 31 de diciembre': 'A', '(Mensual)': 'M'}
    df['FREQ'] = diccionario_periodicidad_sdmx[periodicidad]
    return df

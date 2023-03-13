import os
import sys
import logging
import yaml
import pandas as pd

from iecasdmx.funciones import write_yaml
from iecasdmx.ieca.consulta import Consulta

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Actividad:
    """Una actividad es definida por una lista de consultas a través de su ID en el fichero
    de configuración **'actividades.yaml'**.

    Esta clase ejecutará las consultas y se encargará de hacer las transformaciones pertinentes al
    grupo completo para su correcta modelización en el estandard SDMX.


    Args:
        configuracion_global (:class:`Diccionario`): Configuración común a todas las ejecuciones que se realicen.
        configuracion_actividad (:class:`Diccionario`): Configuración común para toda la actividad.
        mapa_conceptos_codelist (:class:`Diccionario`): Fichero donde se guarda toda la información de
         las jerarquias para su posterior procesamiento
        actividad (:class:`Cadena de Texto`): Nombre de la actividad.

    Attributes:
        consultas (:obj:`Diccionario` de :class:`iecasdmx.consulta.Consulta`): Diccionario que contiene las consultas
         con los datos y metadatos, cuya clave serán los :attr:`iecasdmx.consulta.Consulta.id_consulta`
         correspondientes.
    """

    def __init__(self, configuracion_global, configuracion_actividad, mapa_conceptos_codelist, actividad):
        self.configuracion_global = configuracion_global
        self.configuracion_actividad = {**configuracion_actividad}
        self.mapa_conceptos_codelist = mapa_conceptos_codelist

        self.actividad = actividad

        self.consultas = {}
        self.configuracion = {}

        self.logger = logging.getLogger(f'{self.__class__.__name__} [{actividad}]')
        self.logger.info('Inicializando actividad completa')

    def generar_consultas(self):
        """Inicializa y ejecuta las consultas a la API de BADEA dentro del diccionario :attr:`~.consultas`.

        """

        for consulta in self.configuracion_actividad['consultas']:
            try:
                consulta = Consulta(consulta, self.configuracion_global, self.configuracion_actividad,
                                    self.mapa_conceptos_codelist, self.actividad)

                self.consultas[consulta.id_consulta] = consulta
            except Exception as e:
                raise e
            consulta.ejecutar()

    def ejecutar(self):
        """Aplica las funciones configuradas en el fichero de configuración **'actividades.yaml'** bajo
        la clave **acciones_actividad_completa**.
        """
        self.logger.info('Ejecutando actividad')

        self.generar_fichero_configuracion_actividad()
        self.extender_con_disjuntos()

        self.logger.info('Ejecución finalizada')

    def generar_fichero_configuracion_actividad(self):
        """
        Se genera un fichero con datos relevantes para su posterior uso.
        """
        directorio = os.path.join(self.configuracion_global['directorio_datos_SDMX'], self.actividad)
        fichero = os.path.join(directorio, 'configuracion.yaml')

        if not os.path.exists(directorio):
            os.makedirs(directorio)

        self.logger.info('Creando fichero de configuración de la actividad')

        self.configuracion = {'NOMBRE': self.actividad, 'categoria': self.configuracion_actividad['categoria'],
                              'subcategoria': self.configuracion_actividad['subcategoria'], 'grupos_consultas': {},
                              'variables': [], "metadatos_title": {}, "metadatos_subtitle": {}, 'periodicidad': {}}

        for id_consulta, consulta in self.consultas.items():
            self.configuracion["periodicidad"][consulta.id_consulta] = {'frecuencia': consulta.metadatos['periodicity']}
            df_sorted = consulta.datos.datos.sort_values('D_TEMPORAL_0')
            valid_from = df_sorted["D_TEMPORAL_0"][0]
            valid_to = df_sorted["D_TEMPORAL_0"].iloc[-1]
            if "Anual" in consulta.metadatos['periodicity']:
                valid_from = pd.to_datetime(valid_from, format='%Y')
                valid_to = pd.to_datetime(valid_to, format='%Y')
                valid_to = valid_to + pd.offsets.YearEnd(0)
            else:
                valid_from = pd.to_datetime(valid_from, format='%Y-%m') if "-" in valid_from else pd.to_datetime(
                    valid_from, format='%Y%m')
                valid_to = pd.to_datetime(valid_to, format='%Y-%m') if "-" in valid_to else \
                    pd.to_datetime(valid_to, format='%Y%m')
                valid_to = valid_to + pd.offsets.MonthEnd(0)
            if 'Trimestral' in consulta.metadatos['periodicity']:
                from_month = (valid_from.month - 1) // 3 + 1
                to_month = valid_to.month * 3
                valid_from = valid_from.replace(month=from_month)
                valid_to = valid_to.replace(day=1, month=to_month)
                valid_to = valid_to + pd.offsets.MonthEnd(0)

            self.configuracion["periodicidad"][consulta.id_consulta]["validFrom"] = valid_from.strftime('%Y-%m-%d')
            self.configuracion["periodicidad"][consulta.id_consulta]["validTo"] = valid_to.strftime('%Y-%m-%d')
            self.configuracion["metadatos_title"][consulta.id_consulta] = consulta.metadatos['title']

            self.configuracion["metadatos_subtitle"][consulta.id_consulta] = consulta.metadatos['subtitle']

            if consulta.metadatos['title'] not in self.configuracion['grupos_consultas']:
                self.configuracion['grupos_consultas'][consulta.metadatos['title']] = {
                    'id': str(len(self.configuracion['grupos_consultas']) + 1),
                    'consultas': [id_consulta]}
            else:
                self.configuracion['grupos_consultas'][consulta.metadatos['title']]["consultas"] \
                    .append(id_consulta)
            for columna in consulta.datos.datos_por_observacion.columns:
                if columna not in self.configuracion['variables'] and columna not in ['INDICATOR', 'TEMPORAL',
                                                                                      'OBS_VALUE', 'ESTADO_DATO',
                                                                                      'FREQ', 'OBS_STATUS']:
                    self.configuracion['variables'].append(columna)
        write_yaml(fichero, self.configuracion)
        self.logger.info('Fichero de configuración de la actividad creado y guardado')

    def extender_con_disjuntos(self):
        for consulta in self.consultas:
            self.consultas[consulta].datos.extender_con_disjuntos(self.configuracion['variables'])

    def comprobar_dimensiones_grupo_actividad(self, columnas_grupo, grupo):
        """Comprueba el modelado por titulos en BADEA y muestra por pantalla advertencias sobre las dimensiones
        para facilitar su depuración.

        Args:
            columnas_grupo (:obj:`Lista` de :class:`DataFrame.columns`): Listado de columnas de cada consulta del grupo.
            grupo (:class:`Cadena de Texto`): Titulo del grupo

        """
        self.logger.info('Comprobando dimensiones del grupo de consultas')
        columnas_existentes = set(columna for columnas in columnas_grupo for columna in columnas)

        for columnas in columnas_grupo:
            if set(columnas) != columnas_existentes:
                self.logger.warning('Las dimensiones no coinciden dentro del grupo: %s', grupo)
                self.logger.warning('%s', set(columnas))
                self.logger.warning('%s', columnas_existentes)

        self.logger.info('Comprobación finalizada')

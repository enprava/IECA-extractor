import os
import sys
import logging
import yaml
import pandas as pd

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
        plantilla_configuracion_actividad (:class:`Diccionario`): Configuración por defecto de la actividad.
            Este fichero de configuración extiende a :attr:`~.configuracion_actividad` con las configuraciones que no
            están explicitamente recogidas en este.
        actividad (:class:`Cadena de Texto`): Nombre de la actividad.

    Attributes:
        consultas (:obj:`Diccionario` de :class:`iecasdmx.consulta.Consulta`): Diccionario que contiene las consultas
         con los datos y metadatos, cuya clave serán los :attr:`iecasdmx.consulta.Consulta.id_consulta`
         correspondientes.
    """

    def __init__(self, configuracion_global, configuracion_actividad, plantilla_configuracion_actividad,
                 mapa_conceptos_codelist, actividad):
        self.configuracion_global = configuracion_global
        self.configuracion_actividad = {**plantilla_configuracion_actividad, **configuracion_actividad}
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
        for accion in self.configuracion_actividad['acciones_actividad_completa'].keys():
            if self.configuracion_actividad['acciones_actividad_completa'][accion]:
                getattr(self, accion)()
        self.logger.info('Ejecución finalizada')

    def agrupar_consultas_SDMX(self):
        """Agrupación por titulo para actividades que tienen multiples consultas para una misma serie.
        """
        directorio = os.path.join(self.configuracion_global['directorio_datos_SDMX'], self.actividad)
        self.logger.info('Uniendo datos por titulo')
        nuevas_consultas = {}
        for grupo, informacion_grupo in self.configuracion['grupos_consultas'].items():
            nuevas_consultas[informacion_grupo['id']] = self.consultas[informacion_grupo['consultas'][0]]
            nuevas_consultas[informacion_grupo['id']].id_consulta = informacion_grupo['id']

            self.logger.info('Generando consulta %s con titulo: %s', informacion_grupo['id'], grupo)

            if len(informacion_grupo['consultas']) > 1:
                for consulta in informacion_grupo['consultas'][1:]:
                    nuevas_consultas[
                        informacion_grupo['id']].datos.datos_por_observacion_extension_disjuntos = pd.concat(
                        [nuevas_consultas[informacion_grupo['id']].datos.datos_por_observacion_extension_disjuntos,
                         self.consultas[consulta].datos.datos_por_observacion_extension_disjuntos])

                    for medida in self.consultas[consulta].medidas:
                        if medida not in nuevas_consultas[informacion_grupo['id']].medidas:
                            nuevas_consultas[informacion_grupo['id']].medidas.append(medida)

                    for jerarquia in self.consultas[consulta].jerarquias:
                        if jerarquia not in nuevas_consultas[informacion_grupo['id']].jerarquias:
                            nuevas_consultas[informacion_grupo['id']].jerarquias.append(jerarquia)

        self.consultas = nuevas_consultas

        self.logger.info('Datos por titulo unidos')

    def generar_fichero_configuracion_actividad(self):
        """
        Se genera un fichero con datos relevantes para su posterior uso.
        """
        directorio = os.path.join(self.configuracion_global['directorio_datos_SDMX'], self.actividad)
        fichero = os.path.join(directorio, 'configuracion.yaml')

        if not os.path.exists(directorio):
            os.makedirs(directorio)

        self.logger.info('Creando fichero de configuración de la actividad')
        self.configuracion = {'NOMBRE_DSD': 'DSD_' + self.actividad,
                              'categoria': self.configuracion_actividad['categoria'], 'grupos_consultas': {},
                              'variables': []}
        for id_consulta, consulta in self.consultas.items():
            if consulta.metadatos['title'] not in self.configuracion['grupos_consultas']:
                self.configuracion['grupos_consultas'][consulta.metadatos['title']] = {
                    'id': str(len(self.configuracion['grupos_consultas']) + 1),
                    'consultas': [id_consulta]}
            else:
                self.configuracion['grupos_consultas'][consulta.metadatos['title']]["consultas"] \
                    .append(id_consulta)
            for columna in consulta.datos.datos_por_observacion.columns:
                if columna not in self.configuracion['variables'] and columna not in ['TEMPORAL', 'INDICATOR',
                                                                                      'OBS_VALUE', 'ESTADO_DATO',
                                                                                      'FREQ','OBS_STATUS']:
                    self.configuracion['variables'].append(columna)
        with open(fichero, 'w', encoding='utf-8') as fichero_actividad:
            yaml.dump(self.configuracion, fichero_actividad, allow_unicode=True, sort_keys=False)
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

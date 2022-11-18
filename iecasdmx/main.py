import copy
import os
import time

import pandas as pd
import yaml

from iecasdmx.ieca.actividad import Actividad
from mdmpyclient.mdm import MDM
from mdmpyclient.ckan.ckan import Ckan
import deepl
from ckanapi import RemoteCKAN

import logging

logger = logging.getLogger("deepl")
logger.setLevel(logging.WARNING)

if __name__ == "__main__":
    with open("iecasdmx/configuracion/global.yaml", 'r', encoding='utf-8') as configuracion_global, \
            open("iecasdmx/configuracion/ejecucion.yaml", 'r', encoding='utf-8') as configuracion_ejecucion, \
            open("iecasdmx/configuracion/actividades.yaml", 'r', encoding='utf-8') as configuracion_actividades, \
            open("iecasdmx/configuracion/plantilla_actividad.yaml", 'r',
                 encoding='utf-8') as plantilla_configuracion_actividad, \
            open("sistema_informacion/mapas/conceptos_codelist.yaml", 'r',
                 encoding='utf-8') as mapa_conceptos_codelist_file, \
            open("sistema_informacion/traducciones.yaml", 'r',
                 encoding='utf-8') as traducciones:

        configuracion_global = yaml.safe_load(configuracion_global)
        configuracion_ejecucion = yaml.safe_load(configuracion_ejecucion)
        configuracion_actividades = yaml.safe_load(configuracion_actividades)
        configuracion_plantilla_actividad = yaml.safe_load(plantilla_configuracion_actividad)
        mapa_conceptos_codelist = yaml.safe_load(mapa_conceptos_codelist_file)
        traducciones = yaml.safe_load(traducciones)
        traductor = deepl.Translator('92766a66-fa2a-b1c6-d7dd-ec0750322229:fx')

        agencia = configuracion_global['nodeId']
        if configuracion_global['volcado_ckan']:
            ckan = Ckan(configuracion_global)
        if configuracion_global['reset_ckan']:
            ckan.datasets.remove_all_datasets()

        if configuracion_global['volcado_mdm']:
            controller = MDM(configuracion_global, traductor, True)
            category_scheme = controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0']
            if configuracion_global['reset_ddb']:
                controller.delete_all('ESC01', 'IECA_CAT_EN_ES', '1.0')

        if configuracion_global['translate']:
            controller.category_schemes.data['ESC01']['IECA_CAT_EN_ES']['1.0'].translate()

        for nombre_actividad in configuracion_ejecucion['actividades']:
            actividad = Actividad(configuracion_global, configuracion_actividades[nombre_actividad],
                                  configuracion_plantilla_actividad, mapa_conceptos_codelist, nombre_actividad)
            actividad.generar_consultas()
            actividad.ejecutar()

            cache = yaml.safe_load(open('traducciones.yaml'))
            if configuracion_global['volcado_mdm']:
                # Conversión de Jerarquia a Codelist y Esquemas de conceptos
                for consulta in actividad.consultas.values():
                    for jerarquia in consulta.jerarquias:
                        informacion = mapa_conceptos_codelist[jerarquia.nombre]

                        nombre = informacion['descripcion']
                        descripcion = informacion['descripcion']

                        id_codelist = informacion['codelist']['id']
                        agencia_codelist = informacion['codelist']['agency']
                        version_codelist = informacion['codelist']['version']

                        agencia_concept_scheme = informacion['concept_scheme']['agency']
                        id_concept_scheme = informacion['concept_scheme']['id']
                        version_concept_scheme = informacion['concept_scheme']['version']
                        nombre_concept_scheme_str = id_concept_scheme.replace('CS_', '')[
                                                        0].upper() + id_concept_scheme.replace('CS_', '')[1:].lower()
                        nombre_concept_scheme = {'es': nombre_concept_scheme_str}

                        concepto = informacion['concept_scheme']['concepto']

                        codelist = controller.codelists.add_codelist(agencia_codelist, id_codelist, version_codelist,
                                                                     nombre, descripcion)
                        codelist.add_codes(jerarquia.datos_sdmx)
                        concept_scheme = controller.concept_schemes.add_concept_scheme(agencia_concept_scheme,
                                                                                       id_concept_scheme,
                                                                                       version_concept_scheme,
                                                                                       nombre_concept_scheme, None)
                        concept_scheme.add_concept(concepto, None, descripcion['es'], None)
                    mapa_indicadores = pd.read_csv(
                        os.path.join(configuracion_global['directorio_mapas_dimensiones'], 'INDICATOR'))

                    # Actualización de las medidas
                    try:
                        codelist_medidas = controller.codelists.data[agencia]['CL_UNIT']['1.0']
                    except:
                        codelist_medidas = controller.codelists.add_codelist(agencia, 'CL_UNIT', '1.0',
                                                                             {'es': 'Unidades de Medida (Indicadores)',
                                                                              'en': 'Measurement units (Indicators)'},
                                                                             {'es': 'Unidades de Medida (Indicadores)',
                                                                              'en': 'Measurement units (Indicators)'})
                    # codelist_medidas.init_codes()
                    for consulta in actividad.consultas.values():
                        for medida in consulta.medidas:
                            if medida['des'] in configuracion_global['medidas_reemplazando_obs_status']:
                                continue
                            id_medida = mapa_indicadores[mapa_indicadores['SOURCE'] == medida['des']]['TARGET'].values[
                                0]
                            if id_medida not in codelist_medidas.codes['id']:
                                codelist_medidas.add_code(id_medida, None, medida['des'], None)
                        # codelist_medidas.put()

                controller.concept_schemes.put_all_concept_schemes()
                controller.codelists.put_all_codelists()
                controller.concept_schemes.put_all_data()
                controller.codelists.put_all_data()

                # ## DSD CREACION
                id_dsd = 'DSD_' + nombre_actividad
                agencia_dsd = 'ESC01'
                version_dsd = '1.0'
                nombre_dsd = {'es': actividad.configuracion_actividad['subcategoria']}
                descripcion = None

                variables = copy.deepcopy(actividad.configuracion['variables'])
                try:
                    variables.remove('TEMPORAL')
                except:
                    pass
                try:
                    variables.remove('INDICATOR')
                except:
                    pass
                try:
                    variables.remove('OBS_VALUE')
                except:
                    pass
                try:
                    variables.remove('OBS_STATUS')
                except:
                    pass
                try:
                    variables.remove('FREQ')
                except:
                    pass

                dimensiones = {variable: mapa_conceptos_codelist[variable] for variable in variables}
                print('dimensiones')
                print(dimensiones)
                try:
                    dsd = controller.dsds.data[agencia_dsd][id_dsd][version_dsd]
                except:
                    controller.dsds.put(agencia_dsd, id_dsd, version_dsd, nombre_dsd, descripcion, dimensiones)
                    dsds = controller.dsds.get(init_data=False)
                    dsd = dsds[agencia_dsd][id_dsd][version_dsd]

                # Creación de categoría para la actividad
                category_scheme.init_categories()
                category_scheme.add_category(nombre_actividad, actividad.configuracion_actividad['categoria'],
                                             actividad.configuracion_actividad['subcategoria'], None)
                category_scheme.put()

                # Creación del cubo para la actividad
                for consulta in actividad.consultas.values():
                    cube_id = configuracion_global['nodeId'] + "_" + nombre_actividad + "_" + consulta.id_consulta
                    categories = category_scheme.categories
                    id_cube_cat = \
                        categories[categories['id'] == nombre_actividad]['id_cube_cat'].values[0]

                    id_cubo = controller.cubes.put(cube_id, id_cube_cat, id_dsd,
                                                   consulta.metadatos['subtitle'],
                                                   dimensiones)

                    variables = copy.deepcopy(actividad.configuracion['variables'])
                    mapa = copy.deepcopy(actividad.configuracion['variables'])
                    mapa = ['TIME_PERIOD' if variable == 'TEMPORAL' else variable for variable in mapa]

                    mapping_id = controller.mappings.put(variables, id_cubo,
                                                         nombre_actividad + '_' + consulta.id_consulta)

                    try:
                        mapping = controller.mappings.data[id_cubo].load_cube(
                            consulta.datos.datos_por_observacion_extension_disjuntos)
                    except:
                        controller.mappings.data = controller.mappings.get(True)
                        mapping = controller.mappings.data[id_cubo].load_cube(
                            consulta.datos.datos_por_observacion_extension_disjuntos)

                    id_df = f'DF_{nombre_actividad}_{consulta.id_consulta}'
                    nombre_df = {'es': consulta.metadatos['title']}
                    if consulta.metadatos['subtitle']:
                        nombre_df = {'es': consulta.metadatos['title'] + ': ' + consulta.metadatos['subtitle']}

                    variables_df = ['ID_' + variable if variable != 'OBS_VALUE' else variable for variable in mapa]
                    if 'ID_OBS_STATUS' not in variables_df:
                        variables_df += ['ID_OBS_STATUS']

                    controller.dataflows.put(id_df, agencia, '1.0', nombre_df, None, variables_df, id_cubo, dsd,
                                             category_scheme, nombre_actividad)
                    controller.dataflows.data = controller.dataflows.get(False)
                    try:
                        controller.dataflows.data[agencia][id_df]['1.0'].publish()
                    except:
                        print('está publicado')

                    id_mdf = f'MDF_{nombre_actividad}_{consulta.id_consulta}'
                    controller.metadataflows.put(agencia, id_mdf, '1.0', nombre_df, None)

                    id_mds = f'MDF_{nombre_actividad}_{consulta.id_consulta}'
                    nombre_mds = {'es': consulta.metadatos['title']}
                    if consulta.metadatos['subtitle']:
                        nombre_mds = {'es': consulta.metadatos['title'] + ': ' + consulta.metadatos['subtitle']}
                    categoria = category_scheme.get_category_hierarchy(actividad.actividad)
                    controller.metadatasets.put(agencia, id_mds, nombre_mds, id_mdf, '1.0', 'IECA_CAT_EN_ES', categoria,
                                                '1.0')
                    controller.metadatasets.data[id_mds].put(
                        os.path.join(configuracion_global['directorio_reportes_metadatos'],
                                     actividad.configuracion_actividad['informe_metadatos'] + '.json'))
                    controller.metadatasets.data[id_mds].init_data()
                    controller.metadatasets.data[id_mds].publish_all()
                    #controller.metadatasets.data[id_mds].download_all_reports()
                    if configuracion_global['volcado_ckan']:
                        id_dataset = f'DF_{nombre_actividad}_{consulta.id_consulta}'
                        name_dataset = controller.dataflows.data[agencia][id_df]['1.0'].names['es']
                        ckan.datasets.create(id_dataset.lower(), name_dataset, ckan.orgs.orgs[nombre_actividad.lower()])
                        ckan.resources.create(consulta.datos.datos_por_observacion_extension_disjuntos,
                                              id_dataset, 'csv', id_dataset.lower())
                        controller.metadatasets.data[id_mds].reports.apply(
                            lambda x: ckan.resources.create_from_file(
                                f'{configuracion_global["directorio_metadatos_html"]}/{x.code}.html', x.code, 'html',
                                id_dataset.lower()), axis=1)
        controller.logout()

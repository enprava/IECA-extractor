import json
import os

import yaml
import pandas as pd

pd.options.mode.chained_assignment = None


def read_yaml(path):
    with open(path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def write_yaml(path, data):
    with open(path, 'w', encoding='utf-8') as file:
        yaml.dump(data, file)


def read_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        return json.load(file)


def write_json(path, data):
    with open(path, 'w', encoding='utf-8') as file:
        json.dump(data, file)


def traducir_dataframe_por_variables(df, variables, lenguaje_origen, lenguaje_destino, traductor,
                                     directorio_traducciones):
    if not (df is None):
        df_multilenguaje = {lenguaje_origen: df, lenguaje_destino: df.copy(deep=True)}
        with open(directorio_traducciones, "r") as fichero_traducciones:
            traducciones = yaml.load(fichero_traducciones, Loader=yaml.FullLoader)

            for variable in variables:
                valores_traducidos = []
                for valor in df_multilenguaje[lenguaje_destino][variable].values:
                    if len(valor) == 1:
                        valor = valor[0]
                    try:
                        valores_traducidos.append(traducciones[valor][lenguaje_destino])
                    except:
                        valor_traducido = traductor.translate_text(valor,
                                                                   target_lang=lenguaje_destino if lenguaje_destino != 'en' else 'EN-US').text
                        valores_traducidos.append(valor_traducido)
                        traducciones[valor] = {'en': '', 'es': ''}
                        traducciones[valor][lenguaje_origen] = valor
                        traducciones[valor][lenguaje_destino] = valor_traducido

                        with open(directorio_traducciones, "w+") as fichero_traducciones:
                            yaml.dump(traducciones, fichero_traducciones, sort_keys=True)
                df_multilenguaje[lenguaje_destino][variable] = valores_traducidos
        return df_multilenguaje


def aglutinar_jerarquias_desde_consultas_por_dimension(jerarquias, dimension):
    if dimension not in ['INDICATOR', 'FREQ']:
        jerarquias_filtradas = [jerarquia for jerarquia in jerarquias if jerarquia.nombre == dimension]
        concatenados = pd.concat([jerarquia.datos_sdmx for jerarquia in jerarquias_filtradas], axis=0)
        concatenados.drop_duplicates(subset=['ID'], inplace=True)
        concatenados.dropna(subset=['ID'], inplace=True)

        return concatenados


def mapear_id_por_dimension(df, dimension, directorio_mapas_dimensiones):
    directorio_mapa = os.path.join(directorio_mapas_dimensiones, dimension)

    if not os.path.exists(directorio_mapa):
        df_mapa = pd.DataFrame(columns=["SOURCE", "NAME", "TARGET"])
        df_mapa.to_csv(directorio_mapa, index=False)

    df_mapa = pd.read_csv(directorio_mapa, sep=',', dtype='string', keep_default_na=False)
    df.loc[:, 'ID'] = \
        df.merge(df_mapa, how='left', left_on='COD', right_on='SOURCE')['TARGET'].values
    df.loc[:, 'PARENTCODE'] = \
        df.merge(df_mapa, how='left', left_on='PARENTCODE', right_on='SOURCE')['TARGET'].values
    df.dropna(subset=['ID'], inplace=True)
    df.drop_duplicates(subset=['ID'], inplace=True)
    df[df['ID'] == df['PARENTCODE']]['PARENTCODE'] = None
    df = df[['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']]
    df.columns = ['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']
    return df


def montar_medidas(directorio_mapas_dimensiones):
    mapa_indicadores = pd.read_csv(os.path.join(directorio_mapas_dimensiones, 'INDICATOR'), sep=',', dtype='string')
    mapa_indicadores.drop_duplicates(subset=['TARGET'], inplace=True)
    mapa_indicadores.dropna(subset=['TARGET'], inplace=True)

    indicadores = pd.DataFrame(mapa_indicadores[['TARGET', 'SOURCE', 'COD', 'COD', 'NAME']].values,
                               columns=[['ID', 'NAME', 'DESCRIPTION', 'PARENTCODE', 'ORDER']], dtype='string')

    return indicadores


import unicodedata


def strip_accents(text):
    text = unicodedata.normalize('NFD', text) \
        .encode('ascii', 'ignore') \
        .decode("utf-8")

    return str(text)


def crear_mapeo_por_defecto(descripcion):
    preposiciones = ['A', 'DE', 'POR', 'PARA', 'EN']
    if isinstance(descripcion, pd._libs.missing.NAType):
        return None
    descripcion = descripcion.upper().replace(" ", "_")
    if len(descripcion) >= 15:
        descripcion_reducida = []
        for parte in descripcion.split("_"):
            if parte not in preposiciones:
                if len(parte) >= 4:
                    descripcion_reducida.append(parte[:4])
                else:
                    descripcion_reducida.append(parte)
        descripcion = '_'.join(descripcion_reducida)
    descripcion = descripcion.replace('%', 'PCT')
    descripcion = descripcion.replace('€', 'EUR')
    descripcion = descripcion.replace('(', '')
    descripcion = descripcion.replace(')', '')
    descripcion = descripcion.replace('>=', 'GE')
    descripcion = descripcion.replace('>', 'GT')
    descripcion = descripcion.replace('<=', 'LT')
    descripcion = descripcion.replace('<', 'LE')
    descripcion = descripcion.replace('/', '')
    descripcion = descripcion.replace('"', '')
    descripcion = descripcion.replace(':', '')
    descripcion = descripcion.replace(',', '')
    descripcion = descripcion.replace('+', 'MAS')
    descripcion = descripcion.replace('.', '')
    return strip_accents(descripcion)

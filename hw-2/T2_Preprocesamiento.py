from pathlib import Path
from dateutil.parser import parse
from datetime import timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from helper_funcs import *

def preprocesamiento_casos():
    ts_global = {}
    for file in Path('.').glob('*_global.csv'):
        ts = ts_since_two_per_country(open_global_ts(file))
        ts_name = file.name.split('_')[0]
        ts_global[ts_name] = pd.concat(ts, axis=1)

    first_case, last_case = ts_global['confirmed'].index[0].strftime('%Y-%m-%d'), ts_global['confirmed'].index[-1].strftime('%Y-%m-%d')

    print(f'Series de tiempo Casos COVID para : {list(ts_global.keys())} desde {first_case} hasta {last_case}.\n')
    return ts_global

def preprocesamiento_medidas(medidas_xls, ts_global):
    # Se realizan algunos remplazos para seguir el formato de las series de tiempo de los casos
    replace_cnames_indices = {
        'Slovak Republic' : 'Slovakia',
        'Czech Republic' : 'Czechia',
        'Kyrgyz Republic' : 'Kyrgyzstan', 
        'Cape Verde': 'Cabo Verde', 
        'Taiwan' : 'Taiwan*',
        'South Korea'  : 'Korea, South',
        'United States' : 'US'
    }

    # obtenemos solo medidas
    medidas_ts = {}
    for indice in medidas_xls.sheet_names[:-4]:
            if 'flag' in indice:
                continue

            medida_ts = pd.read_excel(medidas_xls, indice)
            
            for actual, remplazo in replace_cnames_indices.items(): 
                medida_ts.loc[medida_ts['CountryName'] == actual, 'CountryName'] = remplazo

            # Se eliminan las últimas 3 filas basura
            medida_ts = medida_ts.drop(medida_ts.tail(3).index).dropna(thresh=1*len(medida_ts.columns)/5, axis=0)

            # Se filtran por paises que esten la interseccion
            countries_ts = set(medida_ts.CountryName.unique()).intersection(set(ts_global['confirmed'].columns))
            medida_ts = medida_ts[medida_ts['CountryName'].isin(countries_ts)]

            index_name = indice.split('_')[-1]
            print(f'Para el indice {index_name: ^30} existen {len(countries_ts)} paises en Series de tiempo Medidas COVID y Casos COVID.')

            # Utilizamos formato de series de tiempo (index: fecha, columnas: paises)
            medida_ts = medida_ts.drop(labels='CountryCode', axis=1).set_index('CountryName').T
            medida_ts.index = pd.to_datetime([parse(idx) for idx in medida_ts.index])
            medida_ts.columns.name = ''
            
            medidas_ts[index_name] = medida_ts
            
    print('\n')
    
    # obtenemos solo los indices
    indices_ts = {}
    countries_indices_int = {}

    for indice in medidas_xls.sheet_names[-4:]:
        indice_ts = pd.read_excel(medidas_xls, indice)
        
        for actual, remplazo in replace_cnames_indices.items(): 
            indice_ts.loc[indice_ts['CountryName'] == actual, 'CountryName'] = remplazo

        # Se eliminan las últimas 3 filas basura
        indice_ts = indice_ts.drop(indice_ts.tail(3).index).dropna(thresh=1*len(indice_ts.columns)/5, axis=0)

        # Se filtran por paises que esten la interseccion
        countries_ts = set(indice_ts.CountryName.unique()).intersection(set(ts_global['confirmed'].columns))
        indice_ts = indice_ts[indice_ts['CountryName'].isin(countries_ts)]

        index_name = indice.split('_')[-1]
        print(f'Para el indice {index_name: ^30} existen {len(countries_ts)} paises en Series de tiempo Medidas COVID y Casos COVID.')

        # Utilizamos formato de series de tiempo (index: fecha, columnas: paises)
        indice_ts = indice_ts.drop(labels='CountryCode', axis=1).set_index('CountryName').T
        indice_ts.index = pd.to_datetime([parse(idx) for idx in indice_ts.index])
        indice_ts.columns.name = ''
        
        indices_ts[index_name] = indice_ts
        countries_indices_int[index_name] = countries_ts
    return indices_ts, medidas_ts, countries_indices_int

def topics_df_common_countries(ts_global=None, wdi_ind=None, verbose=0):
    topics_indName = pd.read_csv('WDISeries.csv')[['Topic', 'Indicator Name']]
    health_topics = topics_indName[topics_indName['Topic'].str.contains("Health")]['Topic'].unique()

    health_topics = np.delete(health_topics, 5)
    health_topics = np.delete(health_topics, -1)
    health_topics = np.delete(health_topics, -1)
    health_topics = np.delete(health_topics, 0)

    if verbose:
        print('Se utilizan indicadores que tratan los siguientes temas:', health_topics)

    selected_topic_indicators = topics_indName[topics_indName['Topic'].isin(health_topics)]

    if wdi_ind is not None:
        common_countries = set(wdi_ind[(wdi_ind['Indicator Name'].isin(health_indicators))]['Country Name'].unique()).intersection(set(ts_global['confirmed'].columns))
        if verbose:
            print('Existen', len(common_countries), 'paises en la interseccion de fuentes.\n')
        
        return selected_topic_indicators, common_countries

    return selected_topic_indicators

def preprocesamiento_wbd(ts_global):
    wdi_ind = pd.read_csv('WDIData.csv')
    wdi_ind.loc[wdi_ind['Country Name'] == 'United States', 'Country Name'] = 'US'

    selected_topic_indicators, common_countries = topics_df_common_countries(ts_global=ts_global, wdi_ind=wdi_ind, verbose=1)    
    health_indicators = selected_topic_indicators['Indicator Name']

    health_ind_df = wdi_ind[wdi_ind['Indicator Name'].isin(health_indicators) & wdi_ind['Country Name'].isin(common_countries)]
    health_ind_df = health_ind_df.fillna(method='ffill', axis=1)

    health_ind_df['2019'] = health_ind_df['2019'].apply(lambda x: pd.to_numeric(x, errors='coerce'))

    health_ind = health_ind_df[['Country Name', 'Indicator Name', '2019']].reset_index(drop=True)
    return health_ind

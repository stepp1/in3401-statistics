import pandas as pd
from dateutil.parser import parse
from datetime import timedelta
import requests

def download_file(url, filename):
    """
    Helper method handling downloading large files from `url` to `filename`. Returns a pointer to `filename`.
    """
    chunkSize = 1024
    r = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=chunkSize):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return filename


def open_global_ts(path):
    """
    Abre serie de tiempo y cambia el formato para un facil uso.

    Parametros
    ----------
    path : str or pathlib's Path 
        Dirección del archivo csv

    Returns
    -------
    ts: Pandas Dataframe
    """

    ts = pd.read_csv(path)

    ini, end = parse(ts.columns[3]), parse(ts.columns[-1])
    delta = end - ini
    dates = [ini + timedelta(days=i) for i in range(delta.days + 1)]
    ts_cols = [date.strftime('%-m/%-d/%y') for date in dates]

    ts = ts[ts_cols + ['Country']].set_index('Country').T

    ts.index = pd.to_datetime(ts.index)
    ts.sort_index(inplace=True)
    ts.columns.name = ''

    return ts


def ts_since_two_per_country(df):
    df = df.reset_index()

    countries = [df[df[country] > 2][country].reset_index()[[country]]
                 for country in df.columns[1:]]

    return countries


def compute_tasa_incidencia(df, pais):
    """
    Calcula la Tasa de incidencia cada 100.000 habitantes para el país dado.

    TI =   \frac{\text{Total Confirmados Acumulados} \cdot 10^5}{\text{Poblacion}}

    Parametros
    ----------
    df : Pandas Dataframe
        Serie de tiempo de confirmados.
    pais : str
        Nombre del pais, debe ser una columna.

    Returns
    -------
    tasa_incidencia: float
    """
    return df[pais].values[-1] * 1e5 / info_countries[info_countries['Country'] == pais]['Population (2020)'].values


def compute_tasa_contagio(pais, inicio, fin, return_params=False):
    """
    Calcula la Tasa de Contagio cada 100.000 habitantes para un periodo dado.

    \text{Tasa de Contagio} = \frac{\text{cantidad de casos activos} \cdot 10^5}{\text{población con riesgo de infectarse durante el periodo escogido}}

    Parametros
    ----------
    pais : str
        Nombre del pais, debe ser una columna
    incio : datetime
        Inicio del periodo
    end : datetime
        Fin del periodo
    return_params : boolean, optional
        Devuelve parametros involucrados

    Returns
    -------
    tasa_incidencia: float
    """
    inicio = pd.to_datetime(inicio)
    fin = pd.to_datetime(fin)

    # Recordar que la cantidad de muertos, recuperados y confirmados es ACUMULUDADA
    confirmados_horizonte_ini = ts_confirmed[ts_confirmed.index ==
                                             inicio][pais].values
    confirmados_horizonte_end = ts_confirmed[ts_confirmed.index ==
                                             fin][pais].values
    cofirmados_horizonte = confirmados_horizonte_end - confirmados_horizonte_ini

    recuperados_horizonte_ini = ts_recovered[ts_recovered.index ==
                                             inicio][pais].values
    recuperados_horizonte_end = ts_recovered[ts_recovered.index ==
                                             fin][pais].values
    recuperados_horizonte = recuperados_horizonte_end - recuperados_horizonte_ini

    muertos_horizonte_ini = ts_deaths[ts_deaths.index == inicio][pais].values
    muertos_horizonte_end = ts_deaths[ts_deaths.index == fin][pais].values
    muertos_horizonte = muertos_horizonte_end - muertos_horizonte_ini

    poblacion_total = info_countries[info_countries['Country']
                                     == pais]['Population (2020)'].values

    casos_activos_horizonte = cofirmados_horizonte - \
        muertos_horizonte - recuperados_horizonte
    pop_riesgo_horizonte = poblacion_total - muertos_horizonte - \
        recuperados_horizonte - casos_activos_horizonte

    if return_params:
        return casos_activos_horizonte * 1e5, pop_riesgo_horizonte

    return (casos_activos_horizonte / pop_riesgo_horizonte) * 1e5

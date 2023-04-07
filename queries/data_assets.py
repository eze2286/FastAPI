import pandas as pd
import datetime
import numpy as np
import yfinance as yf
from alphacast import Alphacast
import sqlite3

# Obtengo la API_KEY de Alphacast desde un archivo CSV.
with open ("queries/key.csv", "r") as f:
    API_key= f.read()

# Conecccion a Alphacast
alphacast = Alphacast(API_key)

# Creación original de la tabla:

#CREATE TABLE assets(
#Fecha TEXT,
#Activo TEXT,
#Cierre_Aj REAL,
#UNIQUE(Fecha, Activo, Cierre_Aj),
#PRIMARY KEY(Fecha, Activo));

#Carga de datos original en base de datos
def get_dataset():
    df_alphacast = alphacast.datasets.dataset(29828).download_data("pandas")
    return df_alphacast

# con = sqlite3.connect(r'D:\Desktop\Todas mis cosas\Cursos\EXCEL\Python Senior Programmer\FastApi_Proyecto\queries\alphacast.sqlite')
# df_assets = get_dataset()
# df_assets = df_assets[["Fecha", "Activo", "Cierre_Aj"]]
# df_assets.to_sql('assets',con, if_exists="append", index=False)

#Listado de activos en la base de datos
def list_assets(con):
    con = con
    list_assets = pd.read_sql('SELECT Activo FROM assets', con=con)
    list_assets = list_assets.Activo.unique().tolist()
    return list_assets

# Funcion intermedia utilizada en la siguiente funcion para descargar la data del un ticker desde y hasta una fecha
# seleccionada como parametro.

def market_data (ticker):    
    datos = yf.download(ticker)
    return datos

#Funcion para obtener data del ticker seleccionado. Es usada en los metodos de la API
def acciones_urls(ticker):    
    accion = market_data(ticker)
    accion = accion.reset_index()
    accion = accion[["Date", "Adj Close"]]
    accion.insert(1, "Activo", ticker)
    accion = accion.rename(columns={"Date":"Fecha", "Adj Close":"Cierre_Aj"})
    return(accion)


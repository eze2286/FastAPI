from alphacast import Alphacast
import pandas as pd
import sqlite3

# Obtengo la API_KEY de Alphacast desde un archivo CSV.
with open ("queries/key.csv", "r") as f:
    API_key= f.read()

# Conecccion a Alphacast
alphacast = Alphacast(API_key)

# Creación original de la tabla:

# CREATE TABLE "dolar" (
# 	"Date"	TEXT,
# 	"Dolar MEP"	REAL,
# 	"Dolar Oficial"	REAL,
# 	"BLUE"	REAL,
# 	"Dolar CCL"	REAL,
# 	"Dolar Mayorista"	REAL,
# 	"Dolar Solidario"	REAL,
# 	PRIMARY KEY("Date"))

#Carga de datos original en base de datos

# Funcion utilizada en el metodo PUT de la app para obtener la tabla desde Alphacast y actualizar la base de SQL
def data_dolar():
    df_dolar = alphacast.datasets.dataset(5288).download_data("pandas")
    df_dolar = df_dolar.drop_duplicates(subset=["Date"])
    df_dolar = df_dolar.set_index("Date")
    df_dolar = df_dolar[["Dolar MEP", "Dolar Oficial", "BLUE", "Dolar CCL", "Dolar Mayorista", "Dolar Solidario"]]
    
    return df_dolar


# con = sqlite3.connect(r'D:\Desktop\Todas mis cosas\Cursos\EXCEL\Python Senior Programmer\FastApi_Proyecto\queries\alphacast.sqlite')
# df_dolar = data_dolar()
# df_dolar.to_sql('dolar',con, if_exists="append")

#########################################################################################

# Funcion para obtener el tipo de dolar seleccionado como parametro desde la base de datos SQL
def get_individual_dolar(dolar_type):
    con = sqlite3.connect(r'D:\Desktop\Todas mis cosas\Cursos\EXCEL\Python Senior Programmer\FastApi_Proyecto\queries\alphacast.sqlite')
    dolar = pd.read_sql(f'SELECT Date, "{dolar_type}" FROM dolar', con=con)    
    con.close()
    return dolar


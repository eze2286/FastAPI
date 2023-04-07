from alphacast import Alphacast
import pandas as pd
import sqlite3


# Obtengo la API_KEY de Alphacast desde un archivo CSV.
with open ("queries/key.csv", "r") as f:
    API_key= f.read()

# Conecccion a Alphacast
alphacast = Alphacast(API_key)

# Creación original de la tabla:

# CREATE TABLE "inflacion" (
# 	"Date"	TEXT UNIQUE,
# 	"Nivel general"	REAL,
# 	"Alimentos y bebidas no alcohólicas"	REAL,
# 	"Bebidas alcohólicas y tabaco"	REAL,
# 	"Prendas de vestir y calzado"	REAL,
# 	"Vivienda, agua, electricidad y otros combustibles"	REAL,
# 	"Equipamiento y mantenimiento del hogar"	REAL,
# 	"Salud"	REAL,
# 	"Transporte"	REAL,
# 	"Comunicación"	REAL,
# 	"Recreación y cultura"	REAL,
# 	"Educación"	REAL,
# 	"Restaurantes y hoteles"	REAL,
# 	"Bienes y servicios varios"	REAL,
# 	"Estacional"	REAL,
# 	"Núcleo"	REAL,
# 	"Regulados"	REAL,
# 	"Bienes"	REAL,
# 	"Servicios"	REAL,
# 	"country"	TEXT,
# 	PRIMARY KEY("Date"))

#Carga de datos original en base de datos

def data_inflation():
    df_inflation = alphacast.datasets.dataset(5515).download_data("pandas").set_index("Date")
    df_inflation = df_inflation[['Nivel general', 'Alimentos y bebidas no alcohólicas',
       'Bebidas alcohólicas y tabaco', 'Prendas de vestir y calzado',
       'Vivienda, agua, electricidad y otros combustibles',
       'Equipamiento y mantenimiento del hogar', 'Salud', 'Transporte',
       'Comunicación', 'Recreación y cultura', 'Educación',
       'Restaurantes y hoteles', 'Bienes y servicios varios', 'Estacional',
       'Núcleo', 'Regulados', 'Bienes', 'Servicios', 'country']]
    return df_inflation

# df_inflacion = data_inflation()
# print(df_inflacion.columns)
# con = sqlite3.connect(r'D:\Desktop\Todas mis cosas\Cursos\EXCEL\Python Senior Programmer\FastApi_Proyecto\queries\alphacast.sqlite')
# df_inflacion.to_sql('inflacion',con, if_exists="append")

#########################################################################################

# Obtengo la data de la base de datos cargada previamente

def get_inflation(inflation_category):
    con = sqlite3.connect(r'D:\Desktop\Todas mis cosas\Cursos\EXCEL\Python Senior Programmer\FastApi_Proyecto\queries\alphacast.sqlite')
    dolar = pd.read_sql(f'SELECT Date, "{inflation_category}" FROM inflacion', con=con)    
    con.close()
    return dolar

# df = get_inflation("Servicios")
# print(df)

########################################################################################

# Función para la conexion a la base de datos

def get_connection_to_data_base():
    con = sqlite3.connect(r'D:\Desktop\Todas mis cosas\Cursos\EXCEL\Python Senior Programmer\FastApi_Proyecto\queries\alphacast.sqlite')
    return con  


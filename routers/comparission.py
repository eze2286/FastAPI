# Python
import json
import pandas as pd


# FastApi

from fastapi import HTTPException
from fastapi import status
from fastapi import Query
from fastapi import APIRouter

# Modulos

from queries.data_inflacion import get_connection_to_data_base


router = APIRouter()

# Comparar la categoría de inflación y el tipo de dolar seleccionado
@router.get(
    path='/dolar_inflacion/',
    status_code=status.HTTP_200_OK,
    summary="Comparative beetween inflation and dolar by date and category (base=2016)",
    tags=["Comparison"]
    )
async def dolar_vs_inflation_by_date(
    inflation_type:str = Query(...,
                      title="Inflation Type",
                      description="Ingresar una categoría listada en la base de datos para la inflación: Nivel general, Servicios, Núcleo, etc.", 
                      min_length=1,
                      example="Nivel general"),
    dolar_type:str = Query(...,
                      title="Dolar Type",
                      description="Ingresar un tipo de dolar que se encuentre en la base de datos: Dolar CCL, Dolar Mayorista, BLUE, Dolar MEP, Dolar Oficial, Dolar Solidario", 
                      min_length=1,
                      example="BLUE"),    
    date_from:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha desde la cual hacer la consulta: formato 'YYYY-MM-01'", 
                      min_length=1,
                      example="2022-01-01"),
    date_to:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha hasta la cual hacer la consulta: formato 'YYYY-MM-01'", 
                      min_length=1,
                      example="2023-02-01")                                    
    ):
    """
        Obtención de la información comparativa entre la categoria de inflacion y el tipo de dolar seleccionados
        para el periodo elegido
    """
    # Conecto a la base de datos        
    con = get_connection_to_data_base()
    # Obtengo ultimo dato de inflación para tomarlo como tope entre el join del dataset de dolar y el de inflacion
    last_inflation_date = pd.read_sql(f'SELECT inflacion.Date FROM inflacion ORDER BY Date DESC LIMIT 1', con=con).iloc[0].values[0]
    # Compruebo si la fecha elegida por el usuario es mayor a la disponible en el dataset, si es así tomo la ultima disponible
    if pd.to_datetime(date_to) < pd.to_datetime(last_inflation_date):
        last_inflation_date = date_to
    # Hago consultas individuales de cada tabla y luego mergeo las 2 para llevar a mensual la serie diaria del dolar tomadno el ultimo valor y asi poder comparar contra la finclacion mensual
    try:
        df_dolar = pd.read_sql(f'SELECT dolar.Date, dolar."{dolar_type}" FROM dolar WHERE dolar.Date <= "{last_inflation_date}" AND dolar.Date >= "{date_from}" ', con=con)
    except:
        con.close()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tipo de dolar no encontrado")
    # Transformo a mensual la data diaria y agrupo quedandome con el ultimo valor del dolar del mes
    df_dolar["Date"] = df_dolar["Date"].apply(lambda x: x[0:7] + "-" + "01")
    df_dolar = df_dolar.groupby("Date").last()
    # Armo el retorno acumulado
    df_dolar[f"Accumulated-{dolar_type}"] = ((1 + (df_dolar[dolar_type].pct_change())).cumprod()-1)*100
    # Obtengo la tabla de inflacion en base a los parametros ingresados de fecha
    try:
        df_inflation = pd.read_sql(f'SELECT inflacion.Date, inflacion."{inflation_type}" FROM inflacion WHERE inflacion.Date <= "{last_inflation_date}" AND inflacion.Date >= "{date_from}" ', con=con)
    except:
        con.close()
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tipo de inflacion no encontrado")
    # Obtengo el indice acumulado de inflacion.
    df_inflation[f"Accumulated-{inflation_type}"] = ((1 + (df_inflation[inflation_type].pct_change())).cumprod()-1)*100
    df_merged = df_dolar.merge(df_inflation,  on="Date")
    
    if len (df_merged)!=0:
        con.close()
        js =  json.loads(df_merged.to_json(orient = 'records'))
        return js
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data no disponible")


# Hago un JOIN entre tablas para comparar el asset y el tipo de dolar seleccionado
@router.get(
    path='/dolar_asset/',
    status_code=status.HTTP_200_OK,
    summary="Comparative beetween asset and dolar by date",
    tags=["Comparison"]
    )
async def dolar_vs_asset_by_date(
    asset:str = Query(...,
                      title="Asset",
                      description="Ingresar un asset que se encuentre en la base de datos. Ej: TSLA. De no encontrarse se puede ejecutar el metodo PUT correspondiente para traerlo", 
                      min_length=1,
                      example="TSLA"),
    dolar_type:str = Query(...,
                      title="Dolar Type",
                      description="Ingresar un tipo de dolar que se encuentre en la base de datos: Dolar CCL, Dolar Mayorista, BLUE, Dolar MEP, Dolar Oficial, Dolar Solidario", 
                      min_length=1,
                      example="BLUE"),    
    date_from:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha desde la cual hacer la consulta", 
                      min_length=1,
                      example="2022-01-01"),
    date_to:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha hasta la cual hacer la consulta", 
                      min_length=1,
                      example="2023-03-01")                                    
    ):
    """
        Obtención de la información comparativa entre el ASSET y el tipo de dolar seleccionados
        para el periodo elegido
    """
    # Conecto a la base de datos        
    con = get_connection_to_data_base()
    # Obtengo ultimo dato existente en la base de datos tanto para el dolar como para el asset y tomarlo como tope entre el join del dataset de dolar y el de assets
    last_dolar_date = pd.read_sql(f'SELECT dolar.Date FROM dolar ORDER BY Date DESC LIMIT 1', con=con).iloc[0].values[0]
    last_asset_date = pd.read_sql(f'SELECT assets.Fecha FROM assets ORDER BY Fecha DESC LIMIT 1', con=con).iloc[0].values[0]
    # Me quedo con la fecha que sea mas vieja entre el ultimo dato del dolar y de asset
    if last_dolar_date>last_asset_date:
        last_date_to_join = last_asset_date
    else:
        last_date_to_join = last_dolar_date
    # Compruebo si la fecha elegida por el usuario es mayor a la disponible en el dataset, si es así tomo la ultima disponible
    if pd.to_datetime(date_to) < pd.to_datetime(last_date_to_join):
        last_date_to_join = date_to
    # HAGO UN JOIN COMPLEJO
    df = pd.read_sql(f'SELECT dolar.Date, assets.Cierre_Aj, dolar."{dolar_type}" FROM dolar LEFT JOIN assets ON assets.Fecha = dolar.Date WHERE dolar.Date <= "{last_date_to_join}" AND dolar.Date >= "{date_from}" AND assets.Activo = "{asset}" UNION SELECT dolar.Date, assets.Cierre_Aj, dolar."{dolar_type}" FROM assets LEFT JOIN dolar ON assets.Fecha = dolar.Date WHERE assets.fecha IS NULL AND assets.Activo = "{asset}"', con=con)
    #Formateo de data
    df = df.fillna(method='ffill')
    df = df.rename(columns={'Cierre_Aj':asset})
    df = df[~df[asset].isnull()]
    for col in df.columns[1:]:
        df[f"Accumulated-{col.title()}-(%)"] = ((1 + (df[col].pct_change())).cumprod()-1)*100
    df = df[~df[df.columns[0]].isnull()]
    if len (df)!=0:
        con.close()
        js =  json.loads(df.to_json(orient = 'records'))
        return js
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data no disponible")

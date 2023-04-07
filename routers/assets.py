# Python
import json
import pandas as pd

from pydantic import BaseModel

# FastApi

from fastapi import  HTTPException
from fastapi import status
from fastapi import Path, Query
from fastapi import APIRouter

# Modulos
from queries.data_inflacion import get_connection_to_data_base
from queries.data_assets import list_assets, acciones_urls

router = APIRouter()

class Asset(BaseModel):
    asset_code:str


# Obtengo listado de assets que se encuentran actualmente en la base de datos, luego con otros metodos DELETE o PUT se pueden borrar o incorporar Activos
@router.get('/assets',
summary="All Tickers",
tags=["Assets-Tickers"],
status_code=status.HTTP_200_OK
 )
async def get_tickers():
    """
    Listado de activos que se encuentran actualmente en la base de datos
    """
    con = get_connection_to_data_base()
    list_of_assets = list_assets(con)
    con.close()
    if len(list_of_assets) == 0:
        raise HTTPException(status.HTTP_204_NO_CONTENT)
    return list_of_assets


# Obtengo precio y rendimiento del activo seleccionado por fecha

@router.get(
    path='/assets/{asset}',
    summary="Info Asset",
    tags=["Assets-Tickers"])
async def get_asset_info(
    asset:str = Path(..., 
                     title="Activo",
                     description="Ingresar un activo cuyo ticker se encuentre en YFinance. En caso de no estar en la base de datos, se puede incorporar mediante el metodo POST de esta API.", 
                     min_length=1,
                     example="TSLA"),
    date_from:str = Query(...,
                      title="date from",
                      description="Ingresar una fecha desde la cual hacer la consulta", 
                      min_length=1,
                      example="2022-01-01"),
    date_to:str = Query(...,
                      title="date to",
                      description="Ingresar una fecha hasta la cual hacer la consulta", 
                      min_length=1,
                      example="2022-10-01")):
    """
    Obtencion de los precios y rendimientos acumulados por activo y fecha seleccionados.
    """
    con = get_connection_to_data_base()
    df_asset = pd.read_sql(f'SELECT Fecha, Cierre_Aj FROM assets WHERE (Activo="{asset}") AND (Fecha BETWEEN "{date_from}" AND "{date_to}")', con)
    df_asset = df_asset.rename(columns={"Cierre_Aj":asset})
    df_asset[f"Return-{asset}"] = ((1 + (df_asset[asset].pct_change())).cumprod()-1)*100
    con.close()
    if len(df_asset)!=0:        
        js =  json.loads(df_asset.to_json(orient = 'records'))
        return js
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset or Year not found")


#Elimina de la base de datos un asset seleccionado por el usuario en caso de que se encuentre en la misma.
@router.delete(
    path='/asset/{asset_code}',
    summary="Delete a ASSET",
    tags=["Assets-Tickers"]
    )
async def delete_asset(
    asset_code:str = Path(..., 
                          title="Activo", 
                          description="Activo el cuál se busca eliminar de la tabla assets",                          
                          min_length=1, 
                          example="TSLA")
    ):
    """
    Eliminar de la base de datos la información para el activo seleccionado por el usuario de la API
    """
    con = get_connection_to_data_base()
    delete_sentence = f'DELETE FROM assets WHERE Activo="{asset_code}"'
    list_of_assets = list_assets(con)
    if asset_code in list_of_assets:
        cursor = con.cursor()
        cursor.execute(delete_sentence)
        con.commit()
        cursor.close()
        con.close() 
        return {f"{asset_code}" : "was deleted successfuly"}
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")

# Agrego un nuevo Asset seleccionado por el usuario a la base de datos
@router.post(
path='/asset',
summary="Post a ASSET",
status_code=status.HTTP_201_CREATED,
tags=["Assets-Tickers"]
)
async def post_new_asset(
    asset_codes:str = Query(...)
    ):
    """
    Incorporar a la base de datos un activo listado en YFinance.
    """
    con = get_connection_to_data_base()
    asset = asset_codes
    list_of_assets = list_assets(con)
    if asset not in list_of_assets:   
        df = acciones_urls(asset)
        if len(df) != 0:
            df["Fecha"] = df["Fecha"].apply(lambda x: x.strftime("%Y-%m-%d"))
            records = list(df.itertuples(index=False, name=None))
            cursor = con.cursor()
            cursor.executemany('INSERT OR IGNORE INTO assets VALUES(?,?,?);', records)
            con.commit()
            cursor.close()
            con.close()      
            return {f"Asset <<{asset}>> ": "was loaded successfully"}
        else:                
            con.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not in yahoo finance'list")    
    else:
        con.close()
        return ({f"Asset {asset} ": "is already listed in the database"})
    

# FastApi

from fastapi import FastAPI
from fastapi import status

# Modulos
from routers import assets, dolar, inflation, comparission, updates

tags_metadata = [
  {
    "name": "Comparativa de Rendimientos, dolar e inflación",
    "description": "Rendimientos de diferentes activos. Comparativa de dolar e inflación",
    "externalDocs": {
            "description": "Fuente: Alphacast, YFinance",
            "url": "https://www.alphacast.io/datasets/financial-argentina-fx-premiums-daily-5288",
            "url": "https://www.alphacast.io/datasets/inflation-argentina-indec-consumer-price-index-groups-monthly-5515"
        }, 
  }
]

app = FastAPI(title="Comparativa de rendimientos, dolar e inflación histórica", 
              description= "Analisís de rendimiento de diferentes activos y su comparativa contra el dolar y la inflación",
              version= "0.0.1",
              openapi_tags=tags_metadata
                )


# Welcome

@app.get(
    path='/',
    status_code=status.HTTP_200_OK,
    summary="Welcome",
    tags=["Welcome"]
    )
async def welcome_api():
    return "Welcome to API from Assets returns"

# Obtengo toda la funcionalidad de los assets mediante el router correspondiente
app.include_router(assets.router)

# Obtengo toda la funcionalidad de los dolares mediante el router correspondiente
app.include_router(dolar.router)

# Obtengo toda la funcionalidad de la inflacion mediante el router correspondiente
app.include_router(inflation.router)

# Obtengo toda la funcionalidad de las comparaciones mediante el router correspondiente
app.include_router(comparission.router)

# Obtengo toda la funcionalidad de las actualizaciones mediante el router correspondiente
app.include_router(updates.router)
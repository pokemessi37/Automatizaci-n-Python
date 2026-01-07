from fastapi import FastAPI, UploadFile, File
from limpiar_datos import limpiar_csv
from generar_reporte import generar_reporte
import os

app = FastAPI()

# En tu endpoint de FastAPI
@app.post("/procesar")
async def procesar_csv(file: UploadFile = File(...)):
    os.makedirs("data", exist_ok=True)
    
    # Guardar archivo subido
    ruta_csv = "data/ventas.csv"
    with open(ruta_csv, "wb") as f:
        f.write(await file.read())
    
    # Procesar (ahora 100% robusto)
    try:
        filas = limpiar_csv(ruta_csv)
        generar_reporte_word()
        
        return {
            "estado": "ok",
            "filas_procesadas": filas
        }
    except ValueError as e:
        return {
            "estado": "error",
            "mensaje": str(e)
        }

@app.get("/status")
def status():
    return {"servicio": "activo"}

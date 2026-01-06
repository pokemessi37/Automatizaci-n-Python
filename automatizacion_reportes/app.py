from fastapi import FastAPI, UploadFile, File
from limpiar_datos import limpiar_csv
from generar_reporte import generar_reporte

app = FastAPI()

@app.post("/procesar")
async def procesar(file: UploadFile = File(...)):
    with open("data/original.csv", "wb") as f:
        f.write(await file.read())

    filas, resumen = limpiar_csv("data/original.csv")
    generar_reporte(filas)

    return {
        "estado": "ok",
        "filas_procesadas": filas
    }

@app.get("/status")
def status():
    return {"servicio": "activo"}

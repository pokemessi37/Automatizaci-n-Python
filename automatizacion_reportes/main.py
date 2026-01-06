from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import shutil
import os

from limpiar_datos import limpiar_csv
from reporte_word import generar_reporte_word

app = FastAPI()

# Archivos estáticos (CSS, JS, imágenes)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates HTML
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "procesado": False
        }
    )


@app.post("/procesar", response_class=HTMLResponse)
def procesar_csv(request: Request, file: UploadFile = File(...)):
    os.makedirs("data", exist_ok=True)

    ruta_csv = "data/ventas.csv"

    # Guardar archivo subido
    with open(ruta_csv, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Procesar datos
    limpiar_csv(ruta_csv)
    generar_reporte_word()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "procesado": True
        }
    )


@app.get("/descargar/csv")
def descargar_csv():
    return FileResponse(
        path="data/ventas_limpio.csv",
        filename="ventas_limpio.csv",
        media_type="text/csv"
    )


@app.get("/descargar/word")
def descargar_word():
    return FileResponse(
        path="data/reporte_ventas.docx",
        filename="reporte_ventas.docx",
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )

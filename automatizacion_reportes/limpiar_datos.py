import polars as pl
import os
import unicodedata


def normalizar(texto: str) -> str:
    texto = texto.strip().lower()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return texto


def leer_csv_seguro(ruta_csv: str) -> str:
    """
    Lee el CSV en encoding tolerante y lo reescribe en UTF-8 limpio
    """
    temp_utf8 = "data/_upload_utf8.csv"
    os.makedirs("data", exist_ok=True)

    # Leer como texto tolerante (Excel-friendly)
    with open(ruta_csv, "r", encoding="latin-1", errors="replace") as f:
        contenido = f.read()

    # Guardar en UTF-8 real
    with open(temp_utf8, "w", encoding="utf-8") as f:
        f.write(contenido)

    return temp_utf8


def limpiar_csv(ruta_csv: str) -> int:
    ruta_utf8 = leer_csv_seguro(ruta_csv)

    # Intentar separadores comunes
    try:
        df = pl.read_csv(ruta_utf8, separator=";")
        if len(df.columns) == 1:
            raise ValueError
    except:
        df = pl.read_csv(ruta_utf8, separator=",")

    # Normalizar nombres de columnas
    columnas_map = {col: normalizar(col) for col in df.columns}
    df = df.rename(columnas_map)

    # Posibles nombres
    posibles_monto = ["monto", "importe", "total", "precio", "ventas"]
    posibles_cliente = ["cliente", "nombre", "cliente_nombre"]
    posibles_region = ["region", "zona", "area", "provincia"]

    def buscar(posibles):
        for p in posibles:
            if p in df.columns:
                return p
        return None

    col_monto = buscar(posibles_monto)
    col_cliente = buscar(posibles_cliente)
    col_region = buscar(posibles_region)

    if not col_monto or not col_cliente or not col_region:
        raise ValueError(
            f"No se pudieron detectar columnas necesarias.\n"
            f"Columnas encontradas: {df.columns}"
        )

    # Renombrar estándar interno
    df = df.rename({
        col_monto: "monto",
        col_cliente: "cliente",
        col_region: "region"
    })

    # Limpiar monto
    df = df.with_columns(
        pl.col("monto")
        .cast(pl.Utf8)
        .str.replace(",", ".", literal=True)
        .cast(pl.Float64, strict=False)
    )

    df = df.filter(pl.col("monto").is_not_null())
    df = df.filter(pl.col("monto") > 0)

    final_path = "data/ventas_limpio.csv"

    # Guardar con BOM para Excel
    df.write_csv(final_path, separator=";", encoding="utf-8-sig")

    return df.height

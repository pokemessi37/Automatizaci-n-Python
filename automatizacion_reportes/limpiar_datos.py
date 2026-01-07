import polars as pl
import os
import unicodedata
import chardet
from typing import Tuple


def normalizar(texto: str) -> str:
    """Normaliza texto: minúsculas, sin acentos, sin espacios extra."""
    texto = texto.strip().lower()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return texto


def detectar_encoding(ruta_csv: str) -> str:
    """
    Detecta el encoding del archivo CSV automáticamente.
    
    Returns:
        str: Encoding detectado (ej: 'utf-8', 'ISO-8859-1', 'windows-1252')
    """
    with open(ruta_csv, "rb") as f:
        raw_data = f.read(10000)  # Lee primeros 10KB para detectar
    
    resultado = chardet.detect(raw_data)
    encoding = resultado["encoding"]
    confidence = resultado["confidence"]
    
    print(f"🔍 Encoding detectado: {encoding} (confianza: {confidence:.2%})")
    
    # Fallback seguro
    if encoding is None or confidence < 0.7:
        print("⚠️  Confianza baja, usando Latin-1 como fallback")
        return "latin-1"
    
    return encoding


def convertir_a_utf8(ruta_original: str, ruta_destino: str) -> None:
    """
    Convierte cualquier CSV a UTF-8 limpio.
    
    Args:
        ruta_original: Path del CSV original (cualquier encoding)
        ruta_destino: Path donde guardar el CSV en UTF-8
    """
    # Detectar encoding automáticamente
    encoding_original = detectar_encoding(ruta_original)
    
    # Leer con el encoding detectado
    try:
        with open(ruta_original, "r", encoding=encoding_original, errors="replace") as f:
            contenido = f.read()
    except Exception as e:
        print(f"⚠️  Error con {encoding_original}, intentando Latin-1...")
        with open(ruta_original, "r", encoding="latin-1", errors="replace") as f:
            contenido = f.read()
    
    # Escribir en UTF-8 limpio
    with open(ruta_destino, "w", encoding="utf-8", newline="") as f:
        f.write(contenido)
    
    print(f"✅ Archivo convertido a UTF-8: {ruta_destino}")


def detectar_separador(ruta_csv: str) -> str:
    """
    Detecta si el CSV usa ';' o ',' como separador.
    
    Returns:
        str: El separador detectado
    """
    with open(ruta_csv, "r", encoding="utf-8") as f:
        primera_linea = f.readline()
    
    # Contar ocurrencias de cada separador
    count_coma = primera_linea.count(",")
    count_puntocoma = primera_linea.count(";")
    
    separador = ";" if count_puntocoma > count_coma else ","
    print(f"🔍 Separador detectado: '{separador}'")
    
    return separador


def detectar_columnas(df: pl.DataFrame) -> Tuple[str, str, str]:
    """
    Detecta las columnas de monto, cliente y región de forma flexible.
    
    Returns:
        Tuple[str, str, str]: (col_monto, col_cliente, col_region)
    
    Raises:
        ValueError: Si no se encuentran las columnas necesarias
    """
    columnas = [normalizar(col) for col in df.columns]
    
    # Posibles nombres de columnas
    posibles_monto = ["monto", "importe", "total", "precio", "ventas", "venta"]
    posibles_cliente = ["cliente", "nombre", "cliente_nombre", "comprante"]
    posibles_region = ["region", "zona", "area", "provincia", "territorio"]
    
    def buscar(posibles):
        for p in posibles:
            if p in columnas:
                idx = columnas.index(p)
                return df.columns[idx]  # Retornar nombre original
        return None
    
    col_monto = buscar(posibles_monto)
    col_cliente = buscar(posibles_cliente)
    col_region = buscar(posibles_region)
    
    if not col_monto or not col_cliente or not col_region:
        raise ValueError(
            f"❌ No se detectaron las columnas necesarias.\n"
            f"   Columnas encontradas: {df.columns}\n"
            f"   Se requieren columnas tipo: monto, cliente, región"
        )
    
    print(f"✅ Columnas detectadas: monto='{col_monto}', cliente='{col_cliente}', región='{col_region}'")
    
    return col_monto, col_cliente, col_region


def limpiar_csv(ruta_csv: str) -> int:
    """
    Pipeline completo de limpieza de CSV.
    
    Pasos:
    1. Detectar encoding y convertir a UTF-8
    2. Detectar separador
    3. Leer con Polars
    4. Detectar y renombrar columnas
    5. Limpiar datos (monto válido, eliminar nulos)
    6. Guardar CSV limpio con BOM para Excel
    
    Args:
        ruta_csv: Path del CSV original
    
    Returns:
        int: Número de filas procesadas
    """
    os.makedirs("data", exist_ok=True)
    
    # PASO 1: Convertir a UTF-8 (clave para que Polars funcione)
    ruta_utf8_temp = "data/_temp_utf8.csv"
    convertir_a_utf8(ruta_csv, ruta_utf8_temp)
    
    # PASO 2: Detectar separador
    separador = detectar_separador(ruta_utf8_temp)
    
    # PASO 3: Leer con Polars (ahora SÍ está en UTF-8)
    try:
        df = pl.read_csv(ruta_utf8_temp, separator=separador, ignore_errors=True)
    except Exception as e:
        print(f"❌ Error al leer CSV: {e}")
        raise
    
    print(f"📊 CSV leído: {df.height} filas, {df.width} columnas")
    
    # PASO 4: Detectar y renombrar columnas
    col_monto, col_cliente, col_region = detectar_columnas(df)
    
    df = df.select([
        pl.col(col_cliente).alias("cliente"),
        pl.col(col_region).alias("region"),
        pl.col(col_monto).alias("monto")
    ])
    
    # PASO 5: Limpiar datos
    # Convertir monto a float (manejar comas decimales europeas)
    df = df.with_columns(
        pl.col("monto")
        .cast(pl.Utf8)
        .str.replace_all(",", ".")  # Comas decimales → puntos
        .str.strip_chars()          # Eliminar espacios
        .cast(pl.Float64, strict=False)
    )
    
    # Filtrar filas válidas
    filas_originales = df.height
    df = df.filter(pl.col("monto").is_not_null())
    df = df.filter(pl.col("monto") > 0)
    df = df.filter(pl.col("cliente").is_not_null())
    df = df.filter(pl.col("region").is_not_null())
    
    filas_eliminadas = filas_originales - df.height
    print(f"🧹 Limpieza: {filas_eliminadas} filas eliminadas (nulas o monto ≤ 0)")
    
    # PASO 6: Guardar CSV limpio con BOM (compatible con Excel)
    final_path = "data/ventas_limpio.csv"
    df.write_csv(final_path, separator=";")
    
    # Agregar BOM manualmente para Excel
    with open(final_path, "r", encoding="utf-8") as f:
        contenido = f.read()
    with open(final_path, "w", encoding="utf-8-sig") as f:
        f.write(contenido)
    
    print(f"✅ CSV limpio guardado: {final_path} ({df.height} filas)")
    
    # Limpiar archivo temporal
    if os.path.exists(ruta_utf8_temp):
        os.remove(ruta_utf8_temp)
    
    return df.height
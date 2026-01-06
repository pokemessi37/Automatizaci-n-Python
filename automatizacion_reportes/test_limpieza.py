from limpiar_datos import limpiar_csv
from reporte_word import generar_reporte_word

filas = limpiar_csv("data/ventas.csv")
print(f"Filas procesadas correctamente: {filas}")

generar_reporte_word()
print("Reporte Word generado correctamente.")

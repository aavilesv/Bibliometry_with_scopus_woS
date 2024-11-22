import pandas as pd
import re
from nltk.stem import WordNetLemmatizer
import nltk
import time
from collections import Counter

# Descargar recursos necesarios de NLTK
nltk.download('wordnet')  # Para lematización
nltk.download('omw-1.4')  # Recursos adicionales de WordNet

# Inicializa el lematizador
lemmatizer = WordNetLemmatizer()

# Función para limpiar y procesar un término
def limpiar_y_procesar_terminos(termino):
    """
    Limpia y normaliza un término eliminando caracteres especiales,
    preservando guiones en palabras compuestas, y lematizando.
    """
    # 1. Convertir a minúsculas
    termino = termino.lower()
    
    # 2. Permitir guiones en palabras compuestas mientras eliminamos otros caracteres especiales
    termino = re.sub(r"[^\w\s-]", "", termino)
    
    # 3. Quitar espacios adicionales
    termino = termino.strip()
    
    # 4. Dividir en palabras individuales considerando los guiones como parte de las palabras
    palabras = termino.split()
    
    # 5. Lematizar cada palabra (excluyendo los guiones)
    palabras_procesadas = [lemmatizer.lemmatize(p) for p in palabras]
    
    # 6. Reunir palabras procesadas en una frase limpia
    return " ".join(palabras_procesadas)

# Función para procesar una columna completa
def procesar_columna(column):
    """
    Aplica la limpieza y normalización de términos en una columna del DataFrame.
    Separa términos por punto y coma (;) y procesa cada término individualmente.
    """
    return column.fillna("").apply(
        lambda x: "; ".join([limpiar_y_procesar_terminos(termino.strip()) for termino in x.split(";")])
    )

# Función para calcular el Índice de Reducción de Términos Únicos (IRTU)
def calcular_irtu(terminos_antes, terminos_despues):
    """
    Calcula el porcentaje de reducción de términos únicos.
    """
    if terminos_antes == 0:  # Evitar división por cero
        return 0
    return ((terminos_antes - terminos_despues) / terminos_antes) * 100

# Función principal para procesar el DataFrame y calcular métricas
def procesar_dataframe(df, columnas):
    """
    Procesa las columnas especificadas de un DataFrame, aplicando limpieza y normalización.
    Además, calcula métricas como la cantidad de términos únicos antes y después.
    """
    metricas = {}
    
    for columna in columnas:
        print(f"Procesando columna: {columna}")
        
        # Contar términos únicos antes del procesamiento
        terminos_antes = set(";".join(df[columna].fillna("")).split(";"))
        terminos_unicos_antes = len(terminos_antes)
        
        # Procesar la columna
        df[columna] = procesar_columna(df[columna])
        
        # Contar términos únicos después del procesamiento
        terminos_despues = set(";".join(df[columna].fillna("")).split(";"))
        terminos_unicos_despues = len(terminos_despues)
        
        # Calcular el Índice de Reducción de Términos Únicos
        irtu = calcular_irtu(terminos_unicos_antes, terminos_unicos_despues)
        
        # Guardar métricas para esta columna
        metricas[columna] = {
            "Términos Únicos Antes": terminos_unicos_antes,
            "Términos Únicos Después": terminos_unicos_despues,
            "IRTU (%)": irtu
        }
    
    return df, metricas

# Medir el tiempo total de procesamiento
inicio = time.time()

# Cargar el archivo CSV
ruta_archivo = "G:\\Mi unidad\\2024\\Master Solis Granda Luis Eduardo\\data\\wos_scopuskeywords.csv"
df = pd.read_csv(ruta_archivo)
# Cargar el archivo .txt con separador de tabulaciones
#df = pd.read_csv(ruta_archivo, sep="\t")


# Especificar las columnas a procesar cuando es .csv
columnas_a_procesar = ['Index Keywords', 'Author Keywords']

# Aplicar la función a las columnas "DE" y "ID" con es text 
#columnas_a_procesar = ['DE', 'ID']

# Procesar el DataFrame
df_procesado, metricas = procesar_dataframe(df, columnas_a_procesar)

# Mostrar métricas
for columna, valores in metricas.items():
    print(f"\nMétricas para columna: {columna}")
    for key, value in valores.items():
        print(f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}")

# Calcular tiempo total de procesamiento
fin = time.time()
print(f"\nTiempo total de procesamiento: {fin - inicio:.2f} segundos")

# Guardar el DataFrame procesado en un nuevo archivo CSV
ruta_guardado = "G:\\Mi unidad\\2024\\Master Solis Granda Luis Eduardo\\data\\wos_scopuskeywordsfinal.csv"

df_procesado.to_csv(ruta_guardado, index=False) # csv
# Guardar el DataFrame filtrado en un nuevo archivo .txt con el mismo formato
#df.to_csv(ruta_guardado, sep="\t", index=False)

print(f"\nArchivo procesado y guardado en: {ruta_guardado}")

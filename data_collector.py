import requests
import pandas as pd
import datetime
import os                                   # Se utiliza para comprobar la existencia del CSV y escribir el encabezado si no existe
from points import PTS_CRITICOS             # Importamos el diccionario de puntos contenido en el archivo points.py

# ---------------- CONFIGURACIONES ---------------------
API_KEY = "VsSXpy8DfoqsQqBFK88PH38Q90kDFBbE"

# URL base de la API de Flow Segment Data
TOMTOM_API_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

CSV_FILE = 'data_historico.csv'
CSV_HEADERS = ['id_punto',
               'marca_tiempo',
               'tViaje_actual_sec',
               'tViaje_ideal_sec',
               'retraso_sec',
               'confianza']

def obtencion_datos():
    print(f"Iniciando recolección de datos: {datetime.datetime.now()}")
    
    # Lista vacía donde se guardarán todos los nuevos registros
    nuevos_registros = []

    for id_punto, data in PTS_CRITICOS.items():
        
        # Preparamos los parámetros de la API
        parametros = {
            "key": API_KEY,
            "point": f"{data['lat']},{data['lon']}", # Formato lat,lon
            "unit": "kmph" }    # Pedimos la velocidad en km/h

        try:
            # Llama a la API
            response = requests.get(TOMTOM_API_URL, params=parametros)
            response.raise_for_status() # Lanza un error si la solicitud falla (ej. 403, 500)
            
            data_flujo_traf = response.json().get('flowSegmentData')

            if not data_flujo_traf:
                print(f"  -> {id_punto}: No se encontraron datos de flujo.")
                continue

            # Extrae los campos definidos a utilizar
            tViaje_actual = data_flujo_traf.get('currentTravelTime') # Tiempo de viaje actual
            tViaje_ideal = data_flujo_traf.get('freeFlowTravelTime') # Tiempo de viaje ideal

            if tViaje_actual is not None and tViaje_ideal is not None:
                # 4. Calcular el retraso
                delay_sec = tViaje_actual - tViaje_ideal
                
                # 5. Preparar el registro
                timestamp = datetime.datetime.now().isoformat()
                nuevos_registros.append({
                    'id_punto': id_punto,
                    'marca_tiempo': timestamp,
                    'tViaje_actual_sec': tViaje_actual,
                    'tViaje_ideal_sec': tViaje_ideal,
                    'retraso_sec': delay_sec,
                    'confianza': data_flujo_traf.get('confidence')
                })
                print(f"   {id_punto}: Éxito. Confianza: {data_flujo_traf.get('confidence')}")
            
            else:
                 print(f"   {id_punto}: Faltan datos (current o freeflow).")

        except requests.exceptions.RequestException as e:
            print(f"  -> {id_punto}: ERROR en la solicitud: {e}")

    # 6. Guardar todos los registros en el CSV
    if nuevos_registros:
        df = pd.DataFrame(nuevos_registros)
        
        # Comprueba si el archivo ya existe para no repetir encabezados
        file_exists = os.path.isfile(CSV_FILE)
        
        df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False, columns=CSV_HEADERS)

        print(f"Guardados {len(nuevos_registros)} nuevos registros en {CSV_FILE}.\n")
    
# Para ejecutar este archivo directamente desde la terminal
if __name__ == "__main__":
    obtencion_datos()
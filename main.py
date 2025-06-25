import paho.mqtt.client as mqtt
import json
import csv
import threading
import time
import serial
import statistics
import math
from smbus2 import SMBus

# ==== CONFIGURATION MQTT ====

CSV_MQTT_FILE = 'mqtt_data.csv'
CSV_MQTT_HEADERS = ['timestamp', 'device_id', 'temperature', 'humidity']

DEVICE_IDS = [
    "device1", "device2", "device3", "device4", "device5",
    "device6", "device7", "device8", "device9", "device10",
    "device11", "device12", "device13", "device14", "device15"
]

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        device_id = payload['end_device_ids']['device_id']

        if device_id not in DEVICE_IDS:
            return  # Ignore autres devices

        timestamp = payload['received_at']
        data = payload['uplink_message']['decoded_payload']
        temperature = data.get('temperature')
        humidity = data.get('humidity')

        row = [timestamp, device_id, temperature, humidity]
        print(f"MQTT reçu : {row}")

        with open(CSV_MQTT_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)

    except Exception as e:
        print(f"Erreur traitement MQTT : {e}")

def init_csv_mqtt():
    try:
        with open(CSV_MQTT_FILE, 'x', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_MQTT_HEADERS)
    except FileExistsError:
        pass

def mqtt_loop():
    init_csv_mqtt()

    client = mqtt.Client()
    client.username_pw_set("optilac@ttn", "NNSXS.4AD4C525NLU6PS7F7F7YIWNJPDJBKP7R7WTNBC4FI.BU3YI4ABBBBY4KGDE26XY5ZGOIPXGNSSOLW4RCBZWK6JDH5NVVHA")
    client.tls_set(ca_certs="etcsslcertsca-certificates.crt")
    client.on_message = on_message
    client.connect("eu1.cloud.thethings.network", 8883, 60)
    client.subscribe("v3/optilac@ttn/devices/+/up")

    print("MQTT : En écoute...")
    client.loop_forever()

# ==== CONFIGURATION CAPTEURS SERIE ====

port_eau = '/dev/ttyUSB0'
port_ciel = '/dev/ttyUSB1'
baudrate = 57600
timeout = 5
epsilon = 0.98  # Émissivité de l'eau

def initialiser_capteur_eau():
    with serial.Serial(port_eau, baudrate, timeout=timeout) as ser:
        ser.write(b'EMO EMI\n')
        ser.write(b'EMI 0.98\n')
        ser.write(b'AMB REF\n')
        ser.write(b'UNI C\n')

def initialiser_capteur_ciel():
    with serial.Serial(port_ciel, baudrate, timeout=timeout) as ser:
        ser.write(b'EMO TRA\n')
        ser.write(b'TRA 1.00\n')
        ser.write(b'AMB REF\n')
        ser.write(b'UNI C\n')

def lire_donnees_eau():
    with serial.Serial(port_eau, baudrate, timeout=timeout) as ser:
        ser.write(b'TEM \n')
        ser.write(b'RAD \n')
        temp = ser.read_until(b'\r')
        rad = ser.read_until(b'\r')
        return temp.decode().strip(), rad.decode().strip()

def lire_donnees_ciel():
    with serial.Serial(port_ciel, baudrate, timeout=timeout) as ser:
        ser.write(b'TEM \n')
        ser.write(b'RAD \n')
        temp = ser.read_until(b'\r')
        rad = ser.read_until(b'\r')
        return temp.decode().strip(), rad.decode().strip()

def calculer_temperature_reelle(T_mesure, T_ciel, epsilon=0.98):
    return T_mesure + epsilon * (T_ciel - T_mesure)

def moyenne_mesures(capteur_func):
    temp_vals = []
    rad_vals = []

    for _ in range(3):
        temp_str, rad_str = capteur_func()
        try:
            temp = float(temp_str.split()[0])
            rad = float(rad_str.split()[0])
            temp_vals.append(temp)
            rad_vals.append(rad)
        except:
            continue
        time.sleep(10)

    return (
        statistics.mean(temp_vals) if temp_vals else None,
        statistics.mean(rad_vals) if rad_vals else None
    )

# ==== CONFIGURATION MPU6050 ====

MPU6050_ADDR = 0x68
PWR_MGMT_1 = 0x6B
ACCEL_XOUT_H = 0x3B
GYRO_XOUT_H = 0x43

bus = SMBus(1)
bus.write_byte_data(MPU6050_ADDR, PWR_MGMT_1, 0)

# Variables partagées thread-safe pour angle
angle_lock = threading.Lock()
angle = 0.0

def read_word(reg):
    high = bus.read_byte_data(MPU6050_ADDR, reg)
    low = bus.read_byte_data(MPU6050_ADDR, reg + 1)
    value = (high << 8) + low
    if value >= 0x8000:
        value = -((65535 - value) + 1)
    return value

def get_sensor_data():
    ax = read_word(ACCEL_XOUT_H)
    ay = read_word(ACCEL_XOUT_H + 2)
    az = read_word(ACCEL_XOUT_H + 4)
    gx = read_word(GYRO_XOUT_H)
    return ax, ay, az, gx

def mpu6050_loop():
    global angle
    dt = 0.01  # 10 ms = 100Hz
    while True:
        ax, ay, az, gx = get_sensor_data()
        gyro_x = gx / 131.0
        inclination_x = math.atan2(ay, az) * 180 / math.pi
        with angle_lock:
            angle = 0.98 * (angle + gyro_x * dt) + 0.02 * inclination_x
        time.sleep(dt)

def get_current_angle():
    with angle_lock:
        return angle

def moyenne_inclinaison(n=3, delay=10):
    angles = []
    for _ in range(n):
        angles.append(get_current_angle())
        time.sleep(delay)
    return statistics.mean(angles) if angles else None

# ==== BOUCLE CAPTEURS PRINCIPALE (avec inclinaison) ====

def capteurs_loop():
    initialiser_capteur_eau()
    initialiser_capteur_ciel()

    fichier_csv = 'donnees_temperature_eau.csv'

    try:
        with open(fichier_csv, mode='a', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            if file.tell() == 0:
                writer.writerow([
                    'Date', 'Heure',
                    'Teau moy (°C)', 'Tciel moy (°C)', 'Treel moy (°C)', 'Radiance moy (W·m⁻²·sr⁻¹)',
                    'Inclinaison moy (°)',
                    'Teau inst (°C)', 'Tciel inst (°C)', 'Treel inst (°C)', 'Radiance inst (W·m⁻²·sr⁻¹)',
                    'Inclinaison inst (°)'
                ])

            while True:
                now = time.localtime()
                date_str = time.strftime('%Y-%m-%d', now)
                heure_str = time.strftime('%H:%M:%S', now)

                print(f"\n⏰ Début mesure à {date_str} {heure_str}...")

                # Moyenne sur 30 sec (3 mesures 10s)
                T_eau_moy, rad_eau_moy = moyenne_mesures(lire_donnees_eau)
                T_ciel_moy, rad_ciel_moy = moyenne_mesures(lire_donnees_ciel)
                T_reel_moy = calculer_temperature_reelle(T_eau_moy, T_ciel_moy)
                inclinaison_moy = moyenne_inclinaison()

                # Instantané
                temp_eau_str, rad_eau_str = lire_donnees_eau()
                temp_ciel_str, rad_ciel_str = lire_donnees_ciel()

                T_eau_inst = float(temp_eau_str.split()[0])
                T_ciel_inst = float(temp_ciel_str.split()[0])
                rad_eau_inst = float(rad_eau_str.split()[0])
                T_reel_inst = calculer_temperature_reelle(T_eau_inst, T_ciel_inst)
                inclinaison_inst = get_current_angle()

                # Écriture CSV
                writer.writerow([
                    date_str, heure_str,
                    round(T_eau_moy, 2) if T_eau_moy is not None else None,
                    round(T_ciel_moy, 2) if T_ciel_moy is not None else None,
                    round(T_reel_moy, 2) if T_reel_moy is not None else None,
                    round(rad_eau_moy, 2) if rad_eau_moy is not None else None,
                    round(inclinaison_moy, 2) if inclinaison_moy is not None else None,
                    round(T_eau_inst, 2),
                    round(T_ciel_inst, 2),
                    round(T_reel_inst, 2),
                    round(rad_eau_inst, 2),
                    round(inclinaison_inst, 2)
                ])

                print("✅ Mesure enregistrée.")
                time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Programme arrêté manuellement.")
    except Exception as e:
        print(f"❌ Erreur capteurs : {e}")

# ==== MAIN ====

if __name__ == '__main__':
    # Lancer MQTT en thread
    mqtt_thread = threading.Thread(target=mqtt_loop, daemon=True)
    mqtt_thread.start()

    # Lancer MPU6050 en thread daemon
    mpu_thread = threading.Thread(target=mpu6050_loop, daemon=True)
    mpu_thread.start()

    # Lancer la lecture des capteurs série dans thread principal
    capteurs_loop()

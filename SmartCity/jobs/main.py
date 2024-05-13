import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from confluent_kafka import serializing_producer, Producer, SerializingProducer

FROM_CITY_COORDINATES = {"latitude": 40.51872, "longitude": -74.4121}  # edison NJ
TO_CITY_COORDINATES = {"latitude": 39.29038, "longitude": -76.61219}  # Baltimore MD

LATITUDE_INCREMENT = FROM_CITY_COORDINATES['latitude'] - TO_CITY_COORDINATES['latitude'] / 100
LONGITUDE_INCREMENT = FROM_CITY_COORDINATES['longitude'] - TO_CITY_COORDINATES['longitude'] / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = FROM_CITY_COORDINATES.copy()
random.seed(42)


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    # add randomness
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def get_vehicle_emergency_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Flood', 'Medical', 'Police', 'None']),
        'location': location,
        'timestamp': timestamp,
        'status': random.choice(['Active', 'Resolved'])
    }


def get_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 25),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 15),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 120)
    }


def generate_traffic_camera_data(device_id, timestamp, camera_id, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64Encoding',
        'location': location
    }


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(25, 65),
        'direction': 'South',
        'make': 'Audi',
        'model': 'Q5'
    }


def generate_gps_data(device_id, timestamp, vehicle_type='Private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(25, 65),
        'direction': 'South',
        'vehicleType': vehicle_type

    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type  is not serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed : {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_vehicle_data_to_kafka(dataproducer, topic, data):
    # print(data)
    dataproducer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    dataproducer.flush()


def simulate_journey(dataproducer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], 'Cam123',
                                                           vehicle_data['location'])
        weather_data = get_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        vehicle_emergency_data = get_vehicle_emergency_data(device_id, vehicle_data['timestamp'],
                                                            vehicle_data['location'])

        if (vehicle_data['location'][0] <= TO_CITY_COORDINATES['latitude']
                and vehicle_data['location'][1] >= TO_CITY_COORDINATES['longitude']):
            print('Vehicle reached intended location. Ending Simulation ... Good Bye')
            break

        produce_vehicle_data_to_kafka(dataproducer, VEHICLE_TOPIC, vehicle_data)
        produce_vehicle_data_to_kafka(dataproducer, GPS_TOPIC, gps_data)
        produce_vehicle_data_to_kafka(dataproducer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_vehicle_data_to_kafka(dataproducer, WEATHER_TOPIC, weather_data)
        produce_vehicle_data_to_kafka(dataproducer, EMERGENCY_TOPIC, vehicle_emergency_data)
        time.sleep(3)


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'kafka error:{err}'),
    }

    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'Vehicle-123')
    except KeyboardInterrupt:
        print('simulation ended by user')
    except Exception as e:
        print(f'unexpected error occurred {e}')

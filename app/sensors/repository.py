from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
import json

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mongodb_client.getDatabase("DataBase")

    #La informació que volem afegir a la base de mongoDB
    informacio_insert = {
        "id": db_sensor.id,
        "name": sensor.name,
        "latitude": sensor.latitude,
        "longitude": sensor.longitude,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
    }
    mongodb_client.getCollection("Sensors").insert_one(informacio_insert)
    return db_sensor


#Modificat: Creem la funcio record_data
#Volem que els sensors puguin escriure les seves dades a la base
def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    redis.set(sensor_id, json.dumps(data.dict()))
    return json.loads(redis.get(sensor_id))


def get_data(redis: RedisClient, sensor_id: int, db: Session) -> schemas.Sensor:
    #Agafem la base de dades del sensor
    db_sensor = redis.get(sensor_id)
        
    #Si la tenim, retornem les dades més el id i el nom 
    if db_sensor:
        db_dada = json.loads(db_sensor)
        db_dada["id"] = sensor_id
        db_dada["name"] = get_sensor(db, sensor_id).name
        return db_dada

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor



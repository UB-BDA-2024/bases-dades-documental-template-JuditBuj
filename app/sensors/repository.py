from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from bson.son import SON


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

    mongodb_client.insertDoc(sensor.dict())
    return db_sensor


#Modificat: Creem la funcio record_data
#Volem que els sensors puguin escriure les seves dades a la base
def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.SensorData:
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

def delete_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()

    #Eliminem de mongodb el fixer amb id sensor_id
    mongodb_client.deleteOne(sensor_id)

    #Eliminem el de redis també
    redis.delete(sensor_id)

    return db_sensor


def get_sensors_near(redis: RedisClient, mongodb_client: MongoDBClient, db: Session, latitude: float, longitude: float, radius: float):
    #Llista de retorn amb els nears que hi ha
    nears = []
    
    # Creem una query per a que busci aquells valors de latitude i longitude dins del radi rebut
    query = SON([
        ("latitude", SON([
            ("$gte", latitude - radius),
            ("$lte", latitude + radius)
        ])),
        ("longitude", SON([
            ("$gte", longitude - radius),
            ("$lte", longitude + radius)
        ]))
    ])
    
    # Recuperem tots els documents de sensors de la base de dades
    documents = mongodb_client.collection.find(query)

    for document in documents:
        sensor = get_sensor_by_name(db, document["name"])
        sensor_data = get_data(redis, sensor.id, db)
        data = {
            "id": sensor.id,
            "name": sensor.name
        }
        #Actualitzem les dades del sensor
        data.update(sensor_data)

        #Un cop ho tenim, ens guardem el sensor a la llista dels sensors nears
        nears.append(data)

    return nears






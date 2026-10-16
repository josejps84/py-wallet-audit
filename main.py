import asyncio
import json
import logging
import os
from dotenv import load_dotenv
import aio_pika
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

# Logs format setup  
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load env variables 
load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
MONGODB_URL = os.getenv("MONGODB_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
QUEUE_NAME = os.getenv("QUEUE_NAME")


async def process_and_save(message: aio_pika.IncomingMessage, collection) -> None:
    """
    Manage decode logic and mongo insert 
    """
    # Tries to decode json 
    try:
        cuerpo = message.body.decode()
        logger.info(f"Mensaje recibido: {cuerpo}")
        datos = json.loads(cuerpo)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        logger.error(f"Mensaje mal formado. No es un JSON válido. Error: {e}")
        # Message rejected w/o sending it to queue(requeue=False) avoiding infinite loops
        await message.reject(requeue=False)
        return

    # Tries saving in mongo
    try:
        # Insert doc async
        await collection.insert_one(datos)
        logger.info(f"Mensaje guardado exitosamente en Mongo. ID: {datos.get('id', 'N/A')}")
        
        # Confirm to rabbit message processed(ack)
        await message.ack()

    except PyMongoError as e:
        logger.error(f"Error de base de datos al guardar en Mongo: {e}")
        # Si la BD falla temporalmente, devolvemos el mensaje a la cola (requeue=True) 
        # so it could be processed later or by another instance
        await message.nack(requeue=True)
        # Wait a bit, avoiding saturation if DB failed completely
        await asyncio.sleep(2)


async def main():
    logger.info("Iniciando el microservicio...")

    # Async connection to mongo
    mongo_client = AsyncIOMotorClient(MONGODB_URL)
    db = mongo_client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]
    logger.info("Conectado exitosamente a MongoDB.")

    # HiFi connection to rabbit(reconnects automatically if connection fails)
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        
        # Prefetch_count=10 this service will take 10 messages at max
        await channel.set_qos(prefetch_count=10)
        
        # Queue is persistent(durable=True)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        logger.info(f"Escuchando la cola: '{QUEUE_NAME}'. Para salir presiona CTRL+C")

        # Infinite loop to consume messages
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                # Processing is done in a sepparate thread
                asyncio.create_task(process_and_save(message, collection))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Microservicio detenido manualmente por el usuario.")

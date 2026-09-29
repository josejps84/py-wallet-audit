import asyncio
import json
import logging
import os
from dotenv import load_dotenv
import aio_pika
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

# Configuración de Logs profesional
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
MONGODB_URL = os.getenv("MONGODB_URL")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
QUEUE_NAME = os.getenv("QUEUE_NAME")


async def process_and_save(message: aio_pika.IncomingMessage, collection) -> None:
    """
    Gestiona la lógica interna de decodificación e inserción en MongoDB.
    """
    # 1. Intentar decodificar el mensaje JSON
    try:
        cuerpo = message.body.decode()
        logger.info(f"Mensaje recibido: {cuerpo}")
        datos = json.loads(cuerpo)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        logger.error(f"Mensaje mal formado. No es un JSON válido. Error: {e}")
        # Rechazamos el mensaje sin reenviarlo a la cola (requeue=False) para que no cause un bucle infinito
        await message.reject(requeue=False)
        return

    # 2. Intentar guardar en MongoDB
    try:
        # Insertamos el documento de forma asíncrona
        await collection.insert_one(datos)
        logger.info(f"Mensaje guardado exitosamente en Mongo. ID: {datos.get('id', 'N/A')}")
        
        # Confirmamos a RabbitMQ que el mensaje fue procesado con éxito (ACK)
        await message.ack()

    except PyMongoError as e:
        logger.error(f"Error de base de datos al guardar en Mongo: {e}")
        # Si la BD falla temporalmente, devolvemos el mensaje a la cola (requeue=True) 
        # para que se intente procesar más tarde o lo tome otra instancia
        await message.nack(requeue=True)
        # Esperamos un momento para no saturar si la base de datos se cayó por completo
        await asyncio.sleep(2)


async def main():
    logger.info("Iniciando el microservicio...")

    # Conexión asíncrona a MongoDB
    mongo_client = AsyncIOMotorClient(MONGODB_URL)
    db = mongo_client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]
    logger.info("Conectado exitosamente a MongoDB.")

    # Conexión robusta a RabbitMQ (se reconecta automáticamente si se cae la red)
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        
        # Prefetch_count=10 significa que este microservicio tomará un máximo de 10 mensajes
        # a la vez en memoria. No pedirá más hasta que haga ACK/NACK de alguno.
        await channel.set_qos(prefetch_count=10)
        
        # Declaramos la cola asegurando que sea persistente (durable=True)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        logger.info(f"Escuchando la cola: '{QUEUE_NAME}'. Para salir presiona CTRL+C")

        # Bucle infinito para consumir los mensajes
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                # Enviamos el procesamiento a una tarea asíncrona dedicada
                # Esto permite procesar el mensaje 'B' mientras el mensaje 'A' se escribe en Mongo
                asyncio.create_task(process_and_save(message, collection))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Microservicio detenido manualmente por el usuario.")

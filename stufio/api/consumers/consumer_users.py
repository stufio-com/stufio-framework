from typing import Annotated
from fastapi.params import Depends
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.kafka.fastapi import Logger

router = KafkaRouter()


def broker():
    return router.broker


@router.after_startup
async def test(broker: Annotated[KafkaBroker, Depends(broker)]):
    await broker.publish("Hello!", "test")
    

@router.subscriber("test")
@router.publisher("another-topic")
async def hello_http(msg: str, broker: Annotated[KafkaBroker, Depends(broker)], logger: Logger):
    logger.info("Received message from Kafka: %s", broker.message)
    # Process the message and publish to another topic
    # await broker.publish("Hello, Kafka!", "test")
    
    return {"message": "Hello, Kafka!"}

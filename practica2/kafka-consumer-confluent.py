#!/usr/bin/env python

from confluent_kafka import Consumer

if __name__ == '__main__':

    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'localhost:9092',
        #'bootstrap.servers': 'docker02.aulas.eif.urjc.es:9094',
        #'bootstrap.servers': '10.110.100.77',
        # Fixed properties
        'group.id':          'kafka-python-getting-started',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'earliest'
    }
    # bootstrap.servers: indica dónde está el broker de Kafka (localhost:9092).
    # group.id: Define el grupo de consumidores al que pertenece. En Kafka, si varios consumidores tienen el mismo group.id, 
    # se reparten el trabajo de lectura de las particiones del topic.
    # enable.auto.commit': 'false': significa que el programa no confirmará automáticamente a Kafka que ha leído un mensaje. 
    # auto.offset.reset': 'earliest': Indica que, si es la primera vez que este grupo lee el topic, debe empezar desde el 
    # primer mensaje disponible (el más antiguo) en lugar de esperar a los nuevos.

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic])
    # consumer.subscribe([topic]): El consumidor se une al grupo y solicita escuchar el topic "purchases".
    print(consumer.assignment())

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            # El consumidor pregunta a Kafka si hay mensajes nuevos. Esperará hasta 1.0 segundo. Si no hay nada, 
            # devuelve None (imprime "Waiting...").
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                # Topic: El nombre del canal de origen
                # Key: El identificador (ej. el usuario que compró)
                # Value: El contenido del evento (ej. el producto comprado)
                # Decodificación: Se usa .decode('utf-8') porque en Kafka los datos viajan como binarios (tipo binary).
    except KeyboardInterrupt:
        consumer.unsubscribe()
        # consumer.unsubscribe(): Se desconecta del topic.
    finally:
        # Leave group and commit final offsets
        consumer.close()
        # consumer.close(): Es fundamental para informar al broker que el consumidor abandona el grupo.



# Este consumidor es el equivalente manual a lo que Spark hace internamente cuando ejecutas:
# df = spark.readStream.format("kafka").option("subscribe", "purchases").load() [cite: 622, 625]


# Este programa intercambia mensajes con el broker de Kafka de forma síncrona

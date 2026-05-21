#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer

# Genera y envia eventos (simulando compras) a un tema (topic) de Kafka para que una aplicación como Spark 
# Structured Streaming pueda consumirlos después.


if __name__ == '__main__':

    config = {
        # User-specific properties that you must set
        #'bootstrap.servers':'docker02.aulas.eif.urjc.es:9094',
        'bootstrap.servers':'localhost:9092',
        # Fixed properties
        'acks': 'all'
    }
    # bootstrap.servers: Indica la dirección del servidor (broker) de Kafka. En este caso, apunta a localhost:9092
    # acks': 'all': Es una medida de seguridad. El productor esperará a que todos los nodos de Kafka confirmen la 
    # recepción del mensaje para garantizar que no se pierda información.

    try:
        # Create Producer instance
        producer = Producer(config)
        # Se inicializa el objeto que se encargará de gestionar la comunicación y el envío de mensajes hacia el cluster de Kafka.
    except KeyboardInterrupt:
        pass
    finally:
        pass

    
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
    # La función delivery_callback(err, msg) se ejecuta automáticamente cuando Kafka responde al intento de envío
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
            # Si hay un error (err), informa que el mensaje falló.
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                # Si tiene éxito, imprime en consola el topic, la clave y el valor enviado.

    # Produce data by selecting random values from these lists.
    topic = "purchases"
    user_ids = ['eabara-2', 'jsmith-2', 'sgarcia-2', 'jbernard-2', 'htanaka-2', 'awalther-2']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']
    # Topic: Los mensajes se enviarán al canal llamado "purchases"
    # Datos aleatorios: Utiliza listas de usuarios (user_ids) y productos (products) para crear eventos ficticios mediante choice.

    count = 0
    
    try:
        for _ in range(10): 
        # Realiza 10 iteraciones enviando mensajes
            user_id = choice(user_ids)
            product = choice(products)
            producer.produce(topic, product, user_id, callback=delivery_callback)
            count += 1
    except KeyboardInterrupt:
        pass
    finally:
        # Block until the messages are sent.
        producer.poll(10000)
        # producer.poll(10000): Sirve para servir los eventos de callback. 
        # Permite que el programa espere un tiempo para recibir las confirmaciones de entrega de Kafka.
        producer.flush()
        # Es una llamada crítica que bloquea el programa hasta que todos los mensajes que están en la memoria intermedia (buffer) 
        # hayan sido enviados a Kafka. Sin esto, el programa podría cerrarse antes de terminar el envío de los 10 mensajes.
    

# Este programa intercambia mensajes de forma asíncrona.
# En Kafka, un envío síncrono obligaría al programa a detenerse y esperar la confirmación del broker tras cada mensaje antes de 
# continuar con el siguiente. En cambio, este código utiliza mecanismos diseñados para la máxima eficiencia y no bloqueo.

# Evidencias en el código:
# 1. El uso de Callbacks (Asíncrono): La función delivery_callback(err, msg) se define para ejecutarse "después" (en el futuro)
# cuando Kafka responda. El programa principal no espera a que esta función se ejecute para seguir enviando los otros 9 mensajes.

# EJEMPLO SÍNCRONO (Bloqueante)
# for _ in range(10):
#     user_id = choice(user_ids)
#     product = choice(products)
#     
#     # Enviamos el mensaje
#     producer.produce(topic, product, user_id)
    
    # FORZAMOS LA ESPERA: flush() después de CADA mensaje
    # Esto detiene el programa hasta que el broker confirme
#     producer.flush() 
#     print("Mensaje confirmado, enviando el siguiente...")

# EJEMPLO ASÍNCIRONO (este código)
# for _ in range(10):
#     # Produce no bloquea; el bucle termina en milisegundos
#     producer.produce(topic, product, user_id, callback=delivery_callback)

# Solo aquí, al final de todo el trabajo, se vacía el buffer
# producer.flush()
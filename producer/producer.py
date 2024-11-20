from confluent_kafka import Producer
import random
import time

# Configuration Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "user_messages"

# Liste de messages utilisateur simulés dans plusieurs langues
MESSAGES = [
    "I love this product, it's amazing!",  # Anglais
    "Este producto es increíble, me encanta!",  # Espagnol
    "C'est le pire service que j'aie jamais eu.",  # Français
    "Das ist der schlechteste Service, den ich je hatte.",  # Allemand
    "この製品が大好きです、素晴らしいです！",  # Japonais
    "Это ужасное обслуживание, я недоволен.",  # Russe
    "Este é um ótimo produto, estou muito satisfeito.",  # Portugais
    "这是一款非常棒的产品，我非常喜欢！",  # Chinois
    "Ottimo prodotto, sono molto soddisfatto!",  # Italien
    "Bu hizmet mükemmel, teşekkürler!",  # Turc
]

def delivery_report(err, msg):
    """Callback pour signaler l'état de livraison."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    # Configurer le producteur Kafka
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    print("Starting Producer...")

    while True:
        message = random.choice(MESSAGES)
        print(f"Sending message: {message}")
        producer.produce(
            TOPIC_NAME, value=message.encode('utf-8'), callback=delivery_report
        )
        producer.flush()
        time.sleep(2)  # Envoi toutes les 2 secondes

if __name__ == "__main__":
    main()

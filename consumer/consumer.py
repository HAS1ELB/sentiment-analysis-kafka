from confluent_kafka import Consumer, KafkaError

import sys
sys.path.append(r"C:\Users\HP\AppData\Roaming\Python\Python312\site-packages")

from textblob import TextBlob

# Configuration Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "user_messages"
CONSUMER_GROUP = "sentiment_group"

def analyze_sentiment(message):
    """Analyse le sentiment d'un message."""
    analysis = TextBlob(message)
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity == 0:
        return "Neutral"
    else:
        return "Negative"

def main():
    # Configurer le consommateur Kafka
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([TOPIC_NAME])
    print("Starting Consumer...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Timeout de 1 seconde
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            message_text = msg.value().decode('utf-8')
            sentiment = analyze_sentiment(message_text)
            print(f"Message: {message_text} | Sentiment: {sentiment}")

    except KeyboardInterrupt:
        print("Stopping Consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

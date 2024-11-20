from confluent_kafka.admin import AdminClient, NewTopic

# Configuration Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "user_messages"

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Crée un topic Kafka avec le nom spécifié.
    :param topic_name: Nom du topic à créer
    :param num_partitions: Nombre de partitions
    :param replication_factor: Facteur de réplication
    """
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    
    topic_list = [
        NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    ]
    try:
        response = admin_client.create_topics(new_topics=topic_list)
        for topic, future in response.items():
            try:
                future.result()  # Attendre la création du topic
                print(f"Topic '{topic}' créé avec succès.")
            except Exception as e:
                print(f"Erreur lors de la création du topic '{topic}': {e}")
    except Exception as e:
        print(f"Erreur dans la requête de création de topic: {e}")

if __name__ == "__main__":
    create_topic(TOPIC_NAME)

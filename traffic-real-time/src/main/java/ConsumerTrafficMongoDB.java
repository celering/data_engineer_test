import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class ConsumerTrafficMongoDB {
    public static void main(String[] args) {
        // Nombre del topic de Kafka
        String topicName = "processed-traffic-data";

        // Detalles de conexión a MongoDB
        String mongoUri = "mongodb://mongo_container:27017";
        String dbName = "trafficDB";
        String collectionName = "traffic_data";

        // Configuración del consumidor de Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mongo-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Conexión a MongoDB
        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
            // Se conecta a la base de datos y la colección
            MongoDatabase database = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            // Se suscribe al topic de Kafka
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topicName));

            while (true) {
                try {
                    // Se obtienen los registros del topic de Kafka
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        // Se guardan los registros en MongoDB
                        saveToMongoDB(collection, record);
                        System.out.printf("Received message for MongoDB: key = %s, value = %s, offset = %d%n",
                                record.key(), record.value(), record.offset());
                    }
                } catch (Exception e) {
                    System.err.println("Error processing records for MongoDB: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Error connecting to MongoDB: " + e.getMessage());
        }
    }

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static void saveToMongoDB(MongoCollection<Document> collection, ConsumerRecord<String, String> record) {
        try {
            // Se parsea la clave
            String[] keyParts = record.key().split("\\|");
            String id = keyParts[0];
            String fechaStr = keyParts[1];
            Date fecha = new Date(DATE_FORMAT.parse(fechaStr).getTime());

            // Se parsea el valor
            String[] valueParts = record.value().split(", ");
            double vmedAvg = Double.parseDouble(valueParts[0].split("=")[1]);
            int ocupacionMax = Integer.parseInt(valueParts[1].split("=")[1]);
            int count = Integer.parseInt(valueParts[2].split("=")[1]);

            // Se busca el documento existente
            Document existingDoc = collection.find(and(eq("id", id), eq("fecha", fecha))).first();

            // Se actualiza o inserta el documento
            // Si el documento ya existe, se actualiza con los nuevos valores
            if (existingDoc != null) {
                // Se calcula la nueva media de velocidad y el máximo de ocupación
                // Se actualiza el conteo de registros con la suma
                int existingCount = existingDoc.getInteger("count");
                int newCount = existingCount + count;

                double existingVmedAvg = existingDoc.getDouble("vmedAvg");
                double newVmedAvg = ((existingVmedAvg * existingCount) + (vmedAvg * count)) / newCount;

                int existingOcupacionMax = existingDoc.getInteger("ocupacionMax");
                int newOcupacionMax = Math.max(existingOcupacionMax, ocupacionMax);

                // Se actualiza el documento en MongoDB
                collection.updateOne(
                        and(eq("id", id), eq("fecha", fecha)),
                        combine(
                                set("vmedAvg", newVmedAvg),
                                set("ocupacionMax", newOcupacionMax),
                                set("count", newCount),
                                set("offset", record.offset())
                        )
                );
            } else {
                // Si el documento no existe, se inserta
                Document doc = new Document("id", id)
                        .append("fecha", fecha)
                        .append("vmedAvg", vmedAvg)
                        .append("ocupacionMax", ocupacionMax)
                        .append("count", count)
                        .append("offset", record.offset());
                collection.insertOne(doc);
            }
        } catch (Exception e) {
            System.err.println("Error saving record to MongoDB: " + e.getMessage());
        }
    }
}

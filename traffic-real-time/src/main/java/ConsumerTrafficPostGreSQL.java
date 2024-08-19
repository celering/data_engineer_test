import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerTrafficPostGreSQL {
    // Sentencia SQL para insertar o actualizar los datos de tráfico
    private static final String POSTGRES_UPSERT_SQL =
            "INSERT INTO traffic_data (id, fecha, vmed_avg, ocupacion_max, count, record_offset) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (id, fecha) " +
            "DO UPDATE SET vmed_avg = (traffic_data.vmed_avg * traffic_data.count + EXCLUDED.vmed_avg * EXCLUDED.count) / (traffic_data.count + EXCLUDED.count), " +
            "ocupacion_max = GREATEST(traffic_data.ocupacion_max, EXCLUDED.ocupacion_max), " +
            "count = traffic_data.count + EXCLUDED.count, " +
            "record_offset = GREATEST(traffic_data.record_offset, EXCLUDED.record_offset)";

    public static void main(String[] args) {
        // Nombre del tópico de Kafka
        String topicName = "processed-traffic-data";

        // Detalles de conexión a PostgreSQL
        String postgresJdbcUrl = "jdbc:postgresql://my_postgres_container:5432/trafficDB";
        String postgresJdbcUser = "usuario";
        String postgresJdbcPassword = "4132";

        // Configuración del consumidor de Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "postgres-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Conexión a PostgreSQL
        try (Connection postgresConnection = DriverManager.getConnection(postgresJdbcUrl, postgresJdbcUser, postgresJdbcPassword)) {
            // Se suscribe al tópico de Kafka
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topicName));

            while (true) {
                try {
                    // Se obtienen los registros del tópico de Kafka
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        // Se guardan los registros en PostgreSQL
                        saveToPostgreSQL(postgresConnection, record);
                        System.out.printf("Received message for PostgreSQL: key = %s, value = %s, offset = %d%n",
                                record.key(), record.value(), record.offset());
                    }
                } catch (Exception e) {
                    System.err.println("Error processing records for PostgreSQL: " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to PostgreSQL: " + e.getMessage());
        }
    }

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private static void saveToPostgreSQL(Connection connection, ConsumerRecord<String, String> record) {
        // Se parsea la clave y el valor del registro
        try (PreparedStatement pstmt = connection.prepareStatement(POSTGRES_UPSERT_SQL)) {
            String[] keyParts = record.key().split("\\|");
            String id = keyParts[0];
            String fechaStr = keyParts[1];
            Date fecha = new Date(DATE_FORMAT.parse(fechaStr).getTime());

            // Se extraen los valores de la cadena
            String[] valueParts = record.value().split(", ");
            double vmedAvg = Double.parseDouble(valueParts[0].split("=")[1]);
            int ocupacionMax = Integer.parseInt(valueParts[1].split("=")[1]);
            int count = Integer.parseInt(valueParts[2].split("=")[1]);

            // Se insertan los valores en la sentencia SQL
            pstmt.setString(1, id);
            pstmt.setDate(2, fecha);
            pstmt.setDouble(3, vmedAvg);
            pstmt.setInt(4, ocupacionMax);
            pstmt.setInt(5, count);
            pstmt.setLong(6, record.offset());
            // Se ejecuta la sentencia SQL
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error saving record to PostgreSQL: " + e.getMessage());
        } catch (ParseException e) {
            System.err.println("Error parsing date: " + e.getMessage());
        }
    }
}

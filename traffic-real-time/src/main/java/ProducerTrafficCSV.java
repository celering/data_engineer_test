import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class ProducerTrafficCSV {
    public static void main(String[] args) {
        String topicName = "real-time-traffic-data";
        String csvFilePath = "/datos.csv"; // Ruta relativa dentro del classpath

        // Configuración del productor de Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Lectura del archivo CSV
        try (InputStream inputStream = ProducerTrafficCSV.class.getResourceAsStream(csvFilePath);
             BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            if (inputStream == null) {
                System.err.println("File not found: " + csvFilePath);
                return;
            }

            String line;
            // Omite encabezado
            br.readLine();

            while ((line = br.readLine()) != null) {

                // Convierte la línea CSV a JSON
                String json = csvToJson(line);

                // Cada línea del archivo CSV se envía como un mensaje a Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, json);
                producer.send(record);
                System.out.println("Sent message: " + json);

                // Para simular datos en tiempo real, se agrega un retraso de 100 ms
                Thread.sleep(100);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }


    // Método para convertir una línea CSV a JSON
    private static String csvToJson(String line) {
        String[] fields = line.split(";");
        return String.format("{\"id\": \"%s\", \"fecha\": \"%s\", \"tipo_elem\": \"%s\"," +
                        " \"intensidad\": %s, \"ocupacion\": %s, \"carga\": %s, \"vmed\": %s, " +
                        "\"error\": \"%s\", \"periodo_integracion\": %s}",
                fields[0].replace("\"", ""),
                fields[1].replace("\"", ""),
                fields[2].replace("\"", ""),
                fields[3].replace("\"", ""),
                fields[4].replace("\"", ""),
                fields[5].replace("\"", ""),
                fields[6].replace("\"", ""),
                fields[7].replace("\"", ""),
                fields[8].replace("\"", ""));
    }
}

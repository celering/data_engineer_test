import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.KeyValue;

import java.util.Properties;

public class StreamTrafficProcessor {
    public static void main(String[] args) {
        String inputTopic = "real-time-traffic-data";
        String outputTopic = "processed-traffic-data";

        // Configuración de Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "traffic-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder(); // Crea un nuevo builder
        Gson gson = new Gson(); // Crea un nuevo objeto Gson

        // Consume los datos de tráfico
        KStream<String, String> stream = builder.stream(inputTopic);


        // Filtra las líneas urbanas
        KStream<String, String> filteredStream = stream.filter((key, value) -> {
            JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
            return "URB".equals(jsonObject.get("tipo_elem").getAsString());
        });


        // Extrae fecha y asigna como clave el id y la fecha
        KStream<String, String> withDateKey = filteredStream.map((key, value) -> {
            JsonObject jsonObject = gson.fromJson(value, JsonObject.class);
            String id = jsonObject.get("id").getAsString();
            String fecha = jsonObject.get("fecha").getAsString().split(" ")[0]; // Solo la fecha
            String newKey = id + "|" + fecha;
            return new KeyValue<>(newKey, gson.toJson(jsonObject));
        });

        // Agrupar por id y fecha
        KGroupedStream<String, String> groupedStream = withDateKey.groupByKey();

        // Calcula media de velocidad y máximo de ocupación
        KTable<String, String> aggregatedTable = groupedStream.aggregate(
            () -> {
                JsonObject initial = new JsonObject();
                initial.addProperty("vmed_sum", 0);
                initial.addProperty("count", 0);
                initial.addProperty("ocupacion_max", 0);
                return gson.toJson(initial);
            },
            (key, value, aggregateJson) -> {
                JsonObject valueJson = gson.fromJson(value, JsonObject.class);
                JsonObject aggregate = gson.fromJson(aggregateJson, JsonObject.class);

                int vmed = valueJson.get("vmed").getAsInt();
                int ocupacion = valueJson.get("ocupacion").getAsInt();

                int vmedSum = aggregate.get("vmed_sum").getAsInt();
                int count = aggregate.get("count").getAsInt();
                int ocupacionMax = aggregate.get("ocupacion_max").getAsInt();

                vmedSum += vmed;
                count += 1;
                ocupacionMax = Math.max(ocupacionMax, ocupacion);

                JsonObject newAggregate = new JsonObject();
                newAggregate.addProperty("vmed_sum", vmedSum);
                newAggregate.addProperty("count", count);
                newAggregate.addProperty("ocupacion_max", ocupacionMax);

                return gson.toJson(newAggregate);
            },
            Materialized.with(Serdes.String(), Serdes.String())
        ).mapValues(aggregateJson -> {
            // Calcula la media de velocidad y devuelve los datos agregados en formato String
            JsonObject aggregate = gson.fromJson(aggregateJson, JsonObject.class);
            int vmedSum = aggregate.get("vmed_sum").getAsInt();
            int count = aggregate.get("count").getAsInt();
            int ocupacionMax = aggregate.get("ocupacion_max").getAsInt();
            double vmedAvg = (double) vmedSum / count;

            return String.format("vmed_avg=%.2f, ocupacion_max=%d, count=%d", vmedAvg, ocupacionMax, count);
        });

        // Introduce los datos en el topic de salida
        // Peek para verificar que los datos están siendo agregados
        aggregatedTable
                .toStream()
                .peek((key, value) -> System.out.println("Aggregated: key = " + key + ", value = " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Agrega un shutdown hook para cerrar los streams correctamente
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
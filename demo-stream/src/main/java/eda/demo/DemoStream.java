package eda.demo;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Transformer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class DemoStream 
{

    private static JsonObject field(String type, String name) {
        JsonObject filedDesc = new JsonObject();
        filedDesc.addProperty("type", type);
        filedDesc.addProperty("optional", false);
        filedDesc.addProperty("field",name);
        return filedDesc;
    }

    private static String transformMessage(String value) {
        //
        Gson gson = new Gson();
        JsonObject inputMessage = gson.fromJson(value, JsonObject.class);
        //
        // Constructing PostgreSQL connector schema
        //
        JsonObject schema = new JsonObject();
        schema.addProperty("type", "struct");
        schema.addProperty("optional", false);
        schema.addProperty("name", "employee");
        JsonArray fields = new JsonArray();
        fields.add(field("string", "empno"));
        fields.add(field("string", "firstnme"));
        fields.add(field("string", "lastname"));
        fields.add(field("string", "workdept"));
        schema.add("fields", fields);
        //
        // Constructing payload
        //
        JsonObject payload = new JsonObject();
        payload.add("empno",    inputMessage.get("id"));
        payload.add("firstnme", inputMessage.get("firstName"));
        payload.add("lastname", inputMessage.get("lastName"));
        payload.add("workdept", inputMessage.get("department"));
        //
        // Putting together schema and payload
        //
        JsonObject outputMessage = new JsonObject();
        outputMessage.add("schema", schema);
        outputMessage.add("payload", payload);
        String transformedValue = outputMessage.toString();
        //
        return transformedValue;
    }

    private static void processStream(StreamsBuilder builder, String stateStore, String sourceTopic, String sinkTopic ) {
        KStream<String, String> source = builder.stream(sourceTopic);
        source.filter(new Predicate<String, String>(){
            @Override
            public boolean test(String key, String value) { 
                // Placeholder for the case that we have to exclude the record
                return true;
            }
        }).transform(new TransformerSupplier<String, String, KeyValue<String,String>>() {
            public Transformer<String, String, KeyValue<String, String>> get() {
                return new Transformer<String, String, KeyValue<String, String>>() {
                    @Override
                    public void init(ProcessorContext context) {}
                    //
                    @Override
                    public KeyValue<String, String> transform(String key, String value) {
                        return new KeyValue<String, String>(null, transformMessage(value));
                    }
                    //
                    @Override
                    public void close() {}
                };
            }
        }, stateStore).to(sinkTopic);
    }
    
    public static void main( String[] args )
    {
        // Obtaining parameters from the environment

        String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
        String SCRAM_USER = System.getenv("SCRAM_USER");
        String SCRAM_PASSWORD = System.getenv("SCRAM_PASSWORD");
        String TRUSTSTORE_LOCATION = System.getenv("TRUSTSTORE_LOCATION");
        String TRUSTSTORE_PASSWORD = System.getenv("TRUSTSTORE_PASSWORD");
        String SOURCE_TOPIC = System.getenv("SOURCE_TOPIC");
        String SINK_TOPIC = System.getenv("SINK_TOPIC");
        String STREAM_APPLICATION_ID = System.getenv("STREAM_GROUP_ID");
        String STREAM_STATE_STORE_NAME = System.getenv("STREAM_STATE_STORE_NAME");

        // Connection properties

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE_LOCATION);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD);
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, 
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + SCRAM_USER + "\" " +
            "password=\"" + SCRAM_PASSWORD + "\";");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, STREAM_APPLICATION_ID);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Stream builder
        final StreamsBuilder builder = new StreamsBuilder();

        // State store (we need it for the transformation)
        final String stateStore = STREAM_STATE_STORE_NAME;
        StoreBuilder<KeyValueStore<String, String>> storeSupplier = Stores.keyValueStoreBuilder(
			Stores.inMemoryKeyValueStore(stateStore), Serdes.String(), Serdes.String());
        storeSupplier.build();
        builder.addStateStore(storeSupplier);

        // Processing message
        processStream(builder, stateStore, SOURCE_TOPIC, SINK_TOPIC);

        // Topology
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Handler for stopping the Kafka stream when application is stopped with Ctrl+C
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        // Staring the stream - application remains alive until Ctrl+C is pressed
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}

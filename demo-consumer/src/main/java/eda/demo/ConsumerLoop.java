package eda.demo;

import java.time.Duration;
import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerLoop implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private String topic;

    public ConsumerLoop() {


        // Obtaining parameters from the environment
        String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
        String SCRAM_USER = System.getenv("SCRAM_USER");
        String SCRAM_PASSWORD = System.getenv("SCRAM_PASSWORD");
        String TRUSTSTORE_LOCATION = System.getenv("TRUSTSTORE_LOCATION");
        String TRUSTSTORE_PASSWORD = System.getenv("TRUSTSTORE_PASSWORD");
        String CONSUMER_GROUP_ID = System.getenv("CONSUMER_GROUP_ID");
        String TOPIC = System.getenv("SINK_TOPIC");

        // Connection properties
        Properties props = new Properties();
        props.put(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE_LOCATION);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PASSWORD);
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, 
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + SCRAM_USER + "\" " +
            "password=\"" + SCRAM_PASSWORD + "\";");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Topic name
        topic = TOPIC;

        // Consumer instance
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() { 
        System.out.println();
        System.out.println("-------------------------------------------------------------------------------");                             
        try {
            // Subscription to the topic
            consumer.subscribe(Arrays.asList(topic));

            // Poll loop
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record: records) { 
                    // Print message
                    System.out.println(record.value());
                }
            }
        } catch (WakeupException e) {
            // shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
    
}

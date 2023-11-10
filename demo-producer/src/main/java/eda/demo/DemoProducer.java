package eda.demo;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

public class DemoProducer {
    
    private static KafkaProducer<String, String> producer;

    private static void sendMsg(String topic, String id, String firstName, String lastName, String department) {
        String msg = 
            "{" + 
                "\"id\": \""          + id         + "\"," +
                "\"firstName\": \""   + firstName  + "\"," + 
                "\"lastName\": \""    + lastName   + "\"," +
                "\"department\": \""  + department + "\""  + 
            "}";
            
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, id, msg);

        producer.send(record);
        producer.flush();

    }

    public static void main(String[] args) {

        // Obtaining parameters from the environment

        String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
        String SCRAM_USER = System.getenv("SCRAM_USER");
        String SCRAM_PASSWORD = System.getenv("SCRAM_PASSWORD");
        String TRUSTSTORE_LOCATION = System.getenv("TRUSTSTORE_LOCATION");
        String TRUSTSTORE_PASSWORD = System.getenv("TRUSTSTORE_PASSWORD");
        String TOPIC = System.getenv("SOURCE_TOPIC");

        // Preparing connection properties

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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer instance
        producer = new KafkaProducer<String, String>(props);

        // Test messages (example values are taken from the DB2 SAMPLE database)
        sendMsg(TOPIC, "000010", "Christine", "Haas",      "A00");
        sendMsg(TOPIC, "000020", "Michael",   "Thompson",  "B01");
        sendMsg(TOPIC, "000030", "Sally",     "Kwan",      "C01");
        sendMsg(TOPIC, "000040", "John",      "Geyer",     "E01");
        sendMsg(TOPIC, "000050", "Irving",    "Stern",     "D11");
        sendMsg(TOPIC, "000060", "Eva",       "Pulaski",   "D21");
        sendMsg(TOPIC, "000070", "Eileen",    "Henderson", "E11");

        // Close producer
        producer.close();

    }

}
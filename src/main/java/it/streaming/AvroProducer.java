package it.streaming;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import it.model.User;
import it.model.avro.AvroBuilder;
import it.model.avro.GenericAvroBuilder;
import it.model.avro.SpecificAvroBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;

public class AvroProducer<P, C> {
    Logger log = Logger.getLogger(AvroProducer.class);
    private String ip;
    private String port;
    private String topic;
    private Properties props;
    private KafkaProducer<String, P> producer;
    private AvroBuilder<P, User> recordBuilder;

    public AvroProducer(String ip, String port, String topic) {
        this.ip = ip;
        this.port = port;
        this.topic = topic;
        this.props = new Properties();
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.ip + ":" + this.port);
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        this.props.put("schema.registry.url", "http://" + this.ip + ":8081");
        this.producer = new KafkaProducer<>(this.props);

    }

    public Future produce(User user) throws SerializationException {
        if (this.recordBuilder == null) throw new RuntimeException("recordBuilder not configured");
        P build = this.recordBuilder.build(user);
        ProducerRecord<String, P> record = new ProducerRecord<>(this.topic, user.getId(), build);
            return this.producer.send(record);
    }

    public AvroProducer<P, C> withGeneric() {
        this.recordBuilder = new GenericAvroBuilder<>();
        return this;
    }

    public AvroProducer<P, C> withSpecific() {
        this.recordBuilder = new SpecificAvroBuilder<>();
        return this;
    }

}

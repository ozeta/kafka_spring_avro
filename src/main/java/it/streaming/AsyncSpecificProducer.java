package it.streaming;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.Future;


@SuppressWarnings("Duplicates")
@Component
public class AsyncSpecificProducer {
    private static org.slf4j.Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String ip;
    private String port;
    private String topic;
    private Properties props;
    private Properties stringProps;
    private KafkaProducer<String, SpecificAvroUser> userProducer;
    private KafkaProducer<String, String> stringProducer;


    private ApplicationPropertyDAO appDao;

    @Autowired
    public void setAppPropertyDao(ApplicationPropertyDAO appPropertyDao) {
        this.appDao = appPropertyDao;
    }

    @PostConstruct
    public void init() throws Exception {
        log.info("UserProcessor# PRODUCER INITIALIZED");

        this.ip = appDao.getIp();
        this.port = appDao.getPort();
        this.topic = appDao.getAsyncRequestTopic();
        this.props = new Properties();
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.ip + ":" + this.port);
        this.props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + this.ip + ":8081");
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        this.userProducer = new KafkaProducer<>(this.props);


        this.ip = appDao.getIp();
        this.port = appDao.getPort();
        this.stringProps = new Properties();
        this.stringProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.ip + ":" + this.port);
        this.stringProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + this.ip + ":8081");
        this.stringProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.stringProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.stringProducer = new KafkaProducer<>(this.stringProps);

    }

    public Future produce(String key, SpecificAvroUser user) throws SerializationException {
        log.info("UserProcessor# USER PRODUCED");
        ProducerRecord<String, SpecificAvroUser> record = new ProducerRecord<>(appDao.getAsyncRequestTopic(), key, user);
        return this.userProducer.send(record);
    }
    public Future produce(String key, String str) throws SerializationException {
        log.info("UserProcessor# USER PRODUCED");
        ProducerRecord<String, String> record = new ProducerRecord<>(appDao.getAsyncResponseTopic(), key, str);
        return this.stringProducer.send(record);
    }
}

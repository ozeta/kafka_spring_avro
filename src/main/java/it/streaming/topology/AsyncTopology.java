package it.streaming.topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.topology.processors.TopicRequestProcessor;
import it.streaming.topology.processors.TopicResponseProcessor;
import it.streaming.topology.processors.UserProcessor;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.container.AsyncResponse;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("Duplicates")
@Service
public class AsyncTopology {
    private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ApplicationPropertyDAO appDao;
    private Topology requestTopology;
    private Topology responseTopology;
    private Topology userTopology;
    private KafkaStreams userStream;
    private KafkaStreams requestStream;
    private KafkaStreams responseStream;
    private UserProcessor userProcessor;
    private TopicRequestProcessor topicRequestProcessor;
    private TopicResponseProcessor topicResponseProcessor;

    @Autowired
    public void setUserProcessor(UserProcessor userProcessor) {
        this.userProcessor = userProcessor;
    }

    @Autowired
    public void setTopicResponseProcessor(TopicResponseProcessor topicResponseProcessor) {
        this.topicResponseProcessor = topicResponseProcessor;
    }

    @Autowired
    public void setTopicRequestProcessor(TopicRequestProcessor topicRequestProcessor) {
        this.topicRequestProcessor = topicRequestProcessor;
    }

    private void requestStream(StreamsConfig config) {

        Map<String, String> props = new HashMap<>();
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + appDao.getIp() + ":8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        Serializer avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(props, false);
        Deserializer avroDeserialier = new KafkaAvroDeserializer();
        avroDeserialier.configure(props, false);
        Serde<Object> testSerde = Serdes.serdeFrom(avroSerializer, avroDeserialier);


        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();

        StreamsBuilder builder = new StreamsBuilder();
        SpecificAvroSerde<SpecificAvroUser> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort()), false);
        StoreBuilder<KeyValueStore<String, SpecificAvroUser>> requestStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(appDao.getRequestStateStore()),
                Serdes.String(),
                avroSerde);

        requestTopology = builder
                .build()
                .addSource("TOPIC_REQUEST", stringDeserializer, avroDeserialier, appDao.getAsyncRequestTopic())
                .addProcessor("TOPIC_PROCESS",
                        () -> topicRequestProcessor
                        , "TOPIC_REQUEST")
                .addStateStore(requestStore, "TOPIC_PROCESS")
//                .addSink("TOPIC_RESPONSE", appDao.getAsyncResponseTopic(), stringSerializer, stringSerializer, "TOPIC_REQUEST")
        ;


        requestStream = new KafkaStreams(requestTopology, config);
        requestStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor#REQUEST_PROCESSOR# ", throwable);
            throwable.printStackTrace();
        });
        requestStream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(requestStream::close));

    }

    private void responseStream(StreamsConfig config) {

        Map<String, String> props = new HashMap<>();
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + appDao.getIp() + ":8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        Serializer avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(props, false);
        Deserializer avroDeserialier = new KafkaAvroDeserializer();
        avroDeserialier.configure(props, false);


        StringDeserializer stringDeserializer = new StringDeserializer();

        StreamsBuilder builder = new StreamsBuilder();
        SpecificAvroSerde<SpecificAvroUser> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort()), false);
        StoreBuilder<KeyValueStore<String, SpecificAvroUser>> requestStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(appDao.getRequestStateStore()),
                Serdes.String(),
                avroSerde);

        responseTopology = builder
                .build()
                .addSource("TOPIC_RESPONSE", stringDeserializer, stringDeserializer, appDao.getAsyncResponseTopic())
                .addProcessor("TOPIC_RESPONSE_PROCESS",
                        () -> topicResponseProcessor
                        , "TOPIC_RESPONSE")
//                .addStateStore(requestStore, "TOPIC_PROCESS")
//                .addSink("TOPIC_RESPONSE", appDao.getAsyncResponseTopic(), stringSerializer, stringSerializer, "TOPIC_REQUEST")
        ;


        responseStream = new KafkaStreams(responseTopology, config);
        responseStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor#REQUEST_PROCESSOR# ", throwable);
            throwable.printStackTrace();
        });
        responseStream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(responseStream::close));

    }

    public void init() {
        SpecificAvroSerde<SpecificAvroUser> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort()), false);
        StoreBuilder<KeyValueStore<String, String>> responseStore = buildStore(appDao.getResponseStateStore(), Serdes.String(), Serdes.String());

        StreamsConfig config = new StreamsConfig(this.getConfig());
        requestStream(config);
        responseStream(config);

    }

    private Properties getConfig() {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-requestTopology");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appDao.getIp() + ":" + appDao.getPort());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-requestStream");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort());
        return settings;
    }

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
        System.out.println("ciao");
    }

    private <K, V> StoreBuilder<KeyValueStore<K, V>> buildStore(String stateStore, Serde<K> keySerde, Serde<V> valueSerde) {
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(stateStore),
                keySerde,
                valueSerde);
    }

    public KeyValueStore<String, SpecificAvroUser> getUserProcessorKeyValueStore() {
        return this.topicRequestProcessor.getKvStore();
    }

    public ConcurrentHashMap<String, AsyncResponse> getConcurrentHashMap() {
        return this.topicResponseProcessor.getConcurrentHashMap();
    }


}

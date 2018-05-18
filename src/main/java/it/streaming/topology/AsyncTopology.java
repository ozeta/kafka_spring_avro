package it.streaming.topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.topology.processors.TopicRequestProcessor;
import it.streaming.topology.processors.TopicResponseProcessor;
import it.streaming.topology.processors.UserProcessor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

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

    public KafkaStreams getRequestStream() {
        return requestStream;
    }

    public void init() {

        StreamsBuilder builder = new StreamsBuilder();
        SpecificAvroSerde<SpecificAvroUser> serde = new SpecificAvroSerde<>();
        serde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort()), false);

        StoreBuilder<KeyValueStore<String, SpecificAvroUser>> userStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(appDao.getUserStateStore()),
                Serdes.String(),
                serde);

        StoreBuilder<KeyValueStore<String, String>> requestStore = buildStore(appDao.getRequestStateStore(), Serdes.String(), Serdes.String());
        StoreBuilder<KeyValueStore<String, String>> responseStore = buildStore(appDao.getResponseStateStore(), Serdes.String(), Serdes.String());

        StreamsConfig config = new StreamsConfig(this.getConfig());

        // region TOPOLOGY_USER settings
        userTopology = builder
                .build()
                .addSource("USER_SOURCE", appDao.getTopic())
                .addProcessor("USER_Process",
                        () -> userProcessor
                        , "USER_SOURCE")
                .addStateStore(userStore, "USER_Process")
                .addSink("USER_SINK", appDao.getTopologyTopic(), "USER_Process")
        ;
        //endregion

        // region TOPOLOGY_REQUEST settings
        requestTopology = builder
                .build()
                .addSource("REQUEST_SOURCE", appDao.getAsyncRequestTopic())
                .addProcessor("REQUEST_Process",
                        () -> topicRequestProcessor
                        , "REQUEST_SOURCE")
                .addStateStore(requestStore, "REQUEST_Process")
                .addSink("REQUEST_SINK", appDao.getAsyncResponseTopic(), "REQUEST_Process")
        ;
        //endregion

        //region TOPOLOGY_RESPONSE settings
        responseTopology = builder
                .build()
                .addSource("RESPONSE_SOURCE", appDao.getAsyncResponseTopic())
                .addProcessor("RESPONSE_Process",
                        () -> topicResponseProcessor
                        , "RESPONSE_SOURCE")
                .addStateStore(responseStore, "RESPONSE_Process")
        ;
        //endregion


        userStream = new KafkaStreams(requestTopology, config);
        userStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor#REQUEST_PROCESSOR# ", throwable);
            throwable.printStackTrace();
        });

        requestStream = new KafkaStreams(userTopology, config);
        requestStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor#REQUEST_PROCESSOR# ", throwable);
            throwable.printStackTrace();
        });
        responseStream = new KafkaStreams(responseTopology, config);
        responseStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor#RESPONSE_PROCESSOR ", throwable);
            throwable.printStackTrace();
        });
        userStream.start();
/*        requestStream.start();
        responseStream.start();*/
        Runtime.getRuntime().addShutdownHook(new Thread(userStream::close));
        Runtime.getRuntime().addShutdownHook(new Thread(requestStream::close));
        Runtime.getRuntime().addShutdownHook(new Thread(responseStream::close));
    }

    private Properties getConfig() {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-requestTopology");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appDao.getIp() + ":" + appDao.getPort());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-requestStream");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
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

    public KeyValueStore<String, GenericRecord> getUserProcessorKeyValueStore() {
        return this.userProcessor.getKvStore();
    }
    public ConcurrentHashMap<String, AsyncResponse> getConcurrentHashMap() {
        return this.topicResponseProcessor.getConcurrentHashMap();
    }


}

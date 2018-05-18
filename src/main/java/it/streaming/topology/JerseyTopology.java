package it.streaming.topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import it.spring.ApplicationPropertyDAO;
import it.streaming.topology.processors.TopicRequestProcessor;
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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@Service
public class JerseyTopology {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    ApplicationPropertyDAO appPropertyDao;
    private Topology topology1;
    private Topology topology2;
    private KafkaStreams streams1;
    private KafkaStreams streams2;
    private UserProcessor userProcessor;
    private TopicRequestProcessor topicRequestProcessor;

    @Autowired
    public void setUserProcessor(UserProcessor userProcessor) {
        this.userProcessor = userProcessor;
    }

    @Autowired
    public void setTopicRequestProcessor(TopicRequestProcessor topicRequestProcessor) {
        this.topicRequestProcessor = topicRequestProcessor;
    }

    public KafkaStreams getStreams1() {
        return streams1;
    }

    public void init() {


        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, GenericRecord>> storeBuilder1 = buildStore(appPropertyDao.getSchemaRegistryHost(), appPropertyDao.getStateStore1());
        //TODO
        StoreBuilder<KeyValueStore<String, GenericRecord>> storeBuilder2 = buildStore(appPropertyDao.getSchemaRegistryHost(), appPropertyDao.getStateStore2());
//        userStore = storeBuilder.build();
        //region kafka settings
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-topology1");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appPropertyDao.getIp() + ":" + appPropertyDao.getPort());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-streams1");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, appPropertyDao.getSchemaRegistryHost() + ":" + appPropertyDao.getSchemaRegistryPort());
        //endregion
        StreamsConfig config = new StreamsConfig(settings);

        //region TOPOLOGY1 settings
        topology1 = builder
                .build()
                .addSource("SOURCE", appPropertyDao.getTopic())
                .addProcessor("Process",
                        () -> userProcessor
                        , "SOURCE")
                .addStateStore(storeBuilder1, "Process")
                .addSink("SINK", appPropertyDao.getTopologyTopic(), "Process")
        ;
        //endregion


        // region TOPOLOGY2 settings
        topology2 = builder
                .build()
                .addSource("SOURCE", appPropertyDao.getAsyncRequestTopic())
                .addProcessor("Process",
                        () -> topicRequestProcessor
                        , "SOURCE")
                .addStateStore(storeBuilder1, "Process")
                .addSink("SINK", appPropertyDao.getAsyncResponseTopic(), "Process")
        ;
        //endregion
        streams1 = new KafkaStreams(topology1, config);
        streams1.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor# state store", throwable);
            throwable.printStackTrace();
        });
        streams2 = new KafkaStreams(topology2, config);
        streams2.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("UserProcessor# state store", throwable);
            throwable.printStackTrace();
        });
        streams1.start();
        streams2.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams1::close));
        Runtime.getRuntime().addShutdownHook(new Thread(streams2::close));


    }

    @Autowired
    public void setAppPropertyDao(ApplicationPropertyDAO appPropertyDao) {
        this.appPropertyDao = appPropertyDao;
        System.out.println("ciao");
    }

    private StoreBuilder<KeyValueStore<String, GenericRecord>> buildStore(String schemaHost, String stateStore) {
        //region serde config
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                schemaHost + ":8081");

        final Serde<GenericRecord> avroSerde = new GenericAvroSerde();
        avroSerde.configure(serdeConfig, false); // `false` for record values
        //endregion
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(stateStore),
                Serdes.String(),
                avroSerde);

        //return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStore), Serdes.String(), avroSerde);
    }

    public KeyValueStore<String, GenericRecord> getUserProcessorKeyValueStore(){
        return this.userProcessor.getKvStore();
    }


}

package it.streaming.topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import it.spring.ApplicationPropertyDAO;
import it.streaming.topology.processors.UserProcessor;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("Duplicates")
@Service
public class JerseyTopologyParam<P, C, K, V> {

    /*

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    ApplicationPropertyDAO appDao;
    private Topology topology;
    private KafkaStreams streams;

    @Autowired
    UserProcessor userProcessor;

    public KafkaStreams getRequestStream() {
        return streams;
    }

    public void init() {


        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<V, K>> storeBuilder = buildStore(appDao.getSchemaRegistryHost(), appDao.getRequestStateStore());
//        userStore = storeBuilder.build();
        //region kafka settings
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "avro-stream-topology");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appDao.getIp() + ":" + appDao.getPort());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-streams");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort());
        //endregion
        StreamsConfig config = new StreamsConfig(settings);

        //region kafka settings
        topology = builder
                .build()
                .addSource("SOURCE", appDao.getTopic())
                .addProcessor("Process",
                        new ProcessorSupplier() {
                            @Override
                            public Processor get() {
                                return userProcessor;
                            }
                        }
                        , "SOURCE")
                .addStateStore(storeBuilder, "Process")
                .addSink("SINK", appDao.getTopologyTopic(), "Process")
        ;
        //endregion
        streams = new KafkaStreams(topology, config);
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            logger.info(throwable.getMessage());
            throwable.printStackTrace();

        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
        System.out.println("ciao");
    }

    private <K  extends Serdes, V  extends Serdes> StoreBuilder<KeyValueStore<K, V>> buildStore(String schemaHost, String stateStore, K keySerdes, V valueSerdes) {
        //region serde config
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                schemaHost + ":8081");

        final Serde<V> avroSerde = new GenericAvroSerde();
        avroSerde.configure(serdeConfig, false); // `false` for record values
        //endregion



        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(stateStore),
                Serdes.String(),
                avroSerde);
        return null;
        //return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStore), Serdes.String(), avroSerde);
    }
    private StoreBuilder<KeyValueStore<String, GenericRecord>> buildStore(String schemaHost, String stateStore) {
        //region serde config
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                schemaHost + ":8081");

        final Serde<SpecificRecord> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(serdeConfig, false); // `false` for record values
        //endregion
        return Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(stateStore),
                Serdes.String(),
                avroSerde);

        //return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStore), Serdes.String(), avroSerde);
    }

    public Topology getTopology1() {
        return topology;
    }

    public KeyValueStore<K, V> getUserProcessorKeyValueStore(){
        return this.userProcessor.getKvStore();
    }
*/


}

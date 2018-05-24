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
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ws.rs.container.AsyncResponse;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("Duplicates")
@Component
public class AsyncTopology2 {

    private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private TopicRequestProcessor newRequestProcessor;
    private TopicResponseProcessor newResponseProcessor;
    private ApplicationPropertyDAO appDao;
    private Serializer avroSerializer;
    private Deserializer avroDeserialier;
    private StringSerializer stringSerializer;
    private StringDeserializer stringDeserializer;
    private KafkaStreams requestStream;
    private KafkaStreams responseStream;

    public KeyValueStore<String, SpecificRecord> getUsersMap() {
        return this.newResponseProcessor.getResponseStore();
    }


    public ConcurrentHashMap<String, AsyncResponse> getResponseProcessorAsyncMap() {
        return this.newResponseProcessor.getAsyncMap();
    }

    public ConcurrentHashMap<String, SpecificRecord> getResponseProcessorSpecificMap() {
        return this.newResponseProcessor.getSpecificHashMap();
    }

    @Autowired
    public void setNewRequestProcessor(TopicRequestProcessor newRequestProcessor) {
        this.newRequestProcessor = newRequestProcessor;
    }

    @Autowired
    public void setNewResponseProcessor(TopicResponseProcessor newResponseProcessor) {
        this.newResponseProcessor = newResponseProcessor;
    }

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
    }

    private StoreBuilder<KeyValueStore<String, SpecificAvroUser>> buildStore(String stateStore) {
        SpecificAvroSerde<SpecificAvroUser> avroSerde = new SpecificAvroSerde<>();
        avroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort()), false);
        StoreBuilder<KeyValueStore<String, SpecificAvroUser>> requestStore = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(stateStore),
                Serdes.String(),
                avroSerde);
        return requestStore;
    }

    private Properties getConfig(String applicationId) {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appDao.getIp() + ":" + appDao.getPort());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "tmp/kafka-requestStream");
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        settings.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, appDao.getSchemaRegistryHost() + ":" + appDao.getSchemaRegistryPort());
        return settings;
    }

    private Serde<Object> setupSerDes() {
        Map<String, String> props = new HashMap<>();
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + appDao.getIp() + ":8081");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        avroSerializer = new KafkaAvroSerializer();
        avroDeserialier = new KafkaAvroDeserializer();

        avroSerializer.configure(props, false);
        avroDeserialier.configure(props, false);

        stringSerializer = new StringSerializer();
        stringDeserializer = new StringDeserializer();

        return Serdes.serdeFrom(avroSerializer, avroDeserialier);
    }

    @PreDestroy
    public void cleanUp() throws Exception {
        log.info("TOPOLOGY# Spring clean up");
    }

    private KafkaStreams initTopology(String applicationId, String source, String processor, String sink, String store, String sourceTopic, String sinkTopic, Processor proc) {
        StreamsConfig config = new StreamsConfig(this.getConfig(applicationId));
        Serde<Object> avroSerde = this.setupSerDes();

        StreamsBuilder streamBuilder = new StreamsBuilder();

        Topology topology = streamBuilder
                .build()
                .addSource(source, stringDeserializer, avroDeserialier, sourceTopic)
//                .addSource(source, sourceTopic)
                .addProcessor(processor,
                        () -> proc
                        , source);
        if (store != null) topology = topology.addStateStore(this.buildStore(store), processor);
        if (sink != null) topology = topology.addSink(sink, sinkTopic, stringSerializer, avroSerializer, processor);
//        if (sink != null) topology = topology.addSink(sink, sinkTopic, source);
        KafkaStreams stream = new KafkaStreams(topology, config);
        stream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("TOPOLOGY# " + processor, throwable);
            throwable.printStackTrace();
        });
        return stream;
    }


    @PostConstruct
    public void initIt() throws Exception {
//        init();
        requestStream = initTopology("requestTopology",
                "request_source",
                "request_processor",
                "request_sink",
                appDao.getRequestStateStore(),
                appDao.getAsyncRequestTopic(),
                appDao.getAsyncResponseTopic(),
                newRequestProcessor);
        responseStream = initTopology("responseTopology",
                "response_source",
                "response_processor",
                "response_sink",
                appDao.getResponseStateStore(),
                appDao.getAsyncResponseTopic(),
                appDao.getAsyncResponseSinkTopic(),
                newResponseProcessor);

        startStream(requestStream);
        startStream(responseStream);
    }

    private void startStream(KafkaStreams stream) {
        stream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }

}

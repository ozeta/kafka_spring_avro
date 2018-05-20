package it.streaming.topology.processors;


import it.model.StateStoreWrapperSingleton;
import it.model.User;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.AvroProducer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TopicRequestProcessor implements Processor<String, SpecificAvroUser> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;
    private KeyValueStore<String, SpecificAvroUser> kvStore;
    private Random rand = new Random();

    private ApplicationPropertyDAO appDao;

    private ConcurrentHashMap<String, SpecificAvroUser> _cMap = new ConcurrentHashMap<>();

    @Autowired
    public void setAppPropertyDao(ApplicationPropertyDAO appPropertyDao) {
        this.appDao = appPropertyDao;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore(appDao.getRequestStateStore());
        this.context.schedule(appDao.getInitTimeout(), PunctuationType.STREAM_TIME, (timestamp) -> {
            this._cMap.forEach((k, v) -> {
                Boolean b = rand.nextBoolean();
                new AvroProducer<String, String, String>(appDao.getIp(), appDao.getPort(), appDao.getAsyncResponseTopic())
                        .produce(k, b.toString());
//                context.forward(k, b.toString());
                logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: stored and forwarded: " + k + ": " + b.toString());

            });
            this._cMap.keySet().removeAll(this._cMap.keySet());
//            context.commit();
        });
    }

    @Override
    public void process(String k, SpecificAvroUser v) {
        this._cMap.put(k, v);
    }

    @Override
    public void punctuate(long timestamp) {

    }


    @Override
    public void close() {
    }
    public KeyValueStore<String, SpecificAvroUser> getKvStore() {
        return this.kvStore;
    }

}
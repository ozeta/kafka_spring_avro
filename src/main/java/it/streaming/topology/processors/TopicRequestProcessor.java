package it.streaming.topology.processors;


import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.AsyncSpecificProducer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Component
public class TopicRequestProcessor implements Processor<String, SpecificAvroUser> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;
    //    private KeyValueStore<String, SpecificAvroUser> kvStore;
    private Random rand = new Random();

    private ApplicationPropertyDAO appDao;

    private ConcurrentHashMap<String, SpecificAvroUser> _cMap = new ConcurrentHashMap<>();
    private AsyncSpecificProducer asyncProducer;

    @Autowired
    public void setAppPropertyDao(ApplicationPropertyDAO appPropertyDao) {
        this.appDao = appPropertyDao;
    }

    @Autowired
    public void setAsyncProducer(AsyncSpecificProducer asyncProducer) {
        this.asyncProducer = asyncProducer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
//        this.kvStore = (KeyValueStore) context.getStateStore(appDao.getRequestStateStore());
        this.context.schedule(10000, PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            logger.info("UserProcessor# PING?");

//            Optional<String> any = this._cMap.keySet().stream().findAny();
            if (_cMap.size() == 0) {
                logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: NO NEW EVENTS");
            } else {
                this._cMap.forEach((k, v) -> {
                    Boolean b = rand.nextBoolean();
                    Future produce = asyncProducer.produce(k, b.toString());
//                context.forward(k, b.toString());
                    try {
                        logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: " + produce.get().toString());
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error("UserProcessor#TOPIC_REQUEST_PROCESSOR#: ", e);
                    }
                    logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: stored and forwarded: " + k + ": " + b.toString());

                });
                this._cMap.keySet().removeAll(this._cMap.keySet());
            }
//            context.commit();
        });
    }

    @Override
    public void process(String k, SpecificAvroUser v) {
        logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: PROCESSED " + k);
        this._cMap.put(k, v);
    }

    @Override
    public void punctuate(long timestamp) {

    }


    @Override
    public void close() {
    }
/*

    public KeyValueStore<String, SpecificAvroUser> getKvStore() {
        return this.kvStore;
    }
*/

}
package it.streaming.topology.processors;


import it.model.avro.ResponseDto;
import it.spring.ApplicationPropertyDAO;
import org.apache.avro.specific.SpecificRecord;
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
public class TopicRequestProcessor implements Processor<String, SpecificRecord> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ConcurrentHashMap<String, SpecificRecord> specificUsersMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SpecificRecord> enqueuedUsers = new ConcurrentHashMap<>();
    private ApplicationPropertyDAO appDao;
    private ProcessorContext context;
    private Random rand = new Random();
    private KeyValueStore<String, SpecificRecord> store;

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
        this.store = (KeyValueStore) context.getStateStore(appDao.getRequestStateStore());
        logger.info("TOPOLOGY# Request processor started");
        this.context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    /**
     * salva lo specific avro user nella concurrent hashmap
     *
     * @param k
     * @param v
     */
    @Override
    public void process(String k, SpecificRecord v) {
        this.store.put(k, v);
        enqueuedUsers.put(k, v);
        logger.info("TOPOLOGY# REQUEST processing: " + v);
    }

    /**
     * richiamato dallo scheduler, prende il dato dalla concurrent map.
     * genera il booleano e forwarda la nuova struttura <uuid, responsedto> nel sink.
     *
     * @param l
     */

    @Override
    public void punctuate(long l) {
        if (enqueuedUsers.size() == 0) {
//            logger.info("TOPOLOGY# REQUEST# #PUNCTUATE: NO NEW EVENTS");
            return;
        }
        enqueuedUsers.forEach((k, v) -> {
            Boolean b = rand.nextBoolean();
            ResponseDto response = new ResponseDto();
            response.setUuid(k);
            response.setValue(b);
            context.forward(k, response);
            logger.info("TOPOLOGY# RESPONSE# #PUNCTUATE: forwarded: " + response.toString());
        });
        this.enqueuedUsers.keySet().removeAll(this.enqueuedUsers.keySet());
    }

    @Override
    public void close() {

    }

    public ConcurrentHashMap<String, SpecificRecord> getConcurrentHashMap() {
        return this.specificUsersMap;
    }

    public KeyValueStore<String, SpecificRecord> getResponseStore() {
        return this.store;
    }
}


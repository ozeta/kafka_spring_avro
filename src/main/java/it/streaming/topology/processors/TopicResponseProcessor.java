package it.streaming.topology.processors;


import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TopicResponseProcessor implements Processor<String, String> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;

    public KeyValueStore<String, SpecificAvroUser> getUserStore() {
        return userStore;
    }

    private KeyValueStore<String, SpecificAvroUser> userStore;
    private ApplicationPropertyDAO appDao;
    private ConcurrentHashMap<String, AsyncResponse> _tMap = new ConcurrentHashMap<>();

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
    }

//    @Autowired
//    public void setConcurrentMap(ConcurrentHashMap<String, AsyncResponse> map) {
//        this._tMap = map;
//    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        logger.info("UserProcessor#TOPIC_RESPONSE_PROCESSOR INITIALIZED#");

        this.context = context;
        this.userStore = (KeyValueStore) context.getStateStore(appDao.getResponseStateStore());
//            this.kvStore = (KeyValueStore) context.getStateStore(appDao.getUserStateStore());
//        kvStore = (KeyValueStore) context.getStateStore("storage2");
    }

    @Override
    public void process(String uuid, String booleanValue) {
        logger.info("UserProcessor#TOPIC_RESPONSE_PROCESSOR#");
        AsyncResponse asyncResponse = this._tMap.get(uuid);
        if (asyncResponse == null) {
            logger.info("UserProcessor#TOPIC_RESPONSE_PROCESSOR#: no match in map");
            return;
        } else {
            logger.info("UserProcessor#TOPIC_RESPONSE_PROCESSOR#: response:" + asyncResponse.toString());

        }

        SpecificAvroUser user = userStore.get(uuid);
        if (booleanValue.equalsIgnoreCase("true")) {
            logger.info("UserProcessor#TOPIC_RESPONSE_PROCESSOR#: SENDING: " + uuid + ": " + user.toString());
            asyncResponse.resume(Response.status(Response.Status.CREATED).entity(user).build());
        } else {
            logger.info("UserProcessor#TOPIC_RESPONSE_PROCESSOR#: NOT SENDING: " + uuid);
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(user).build());
        }

    }

    @Override
    public void punctuate(long timestamp) {
        //deprecated
    }

    @Override
    public void close() {
        //kvStore.close();
    }

    public ConcurrentHashMap<String, AsyncResponse> getConcurrentHashMap() {
        return _tMap;
    }
}
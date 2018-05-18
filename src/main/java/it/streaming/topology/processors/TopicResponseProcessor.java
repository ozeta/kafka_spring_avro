package it.streaming.topology.processors;


import it.model.StateStoreWrapperSingleton;
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
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TopicResponseProcessor implements Processor<String, String> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;
    private KeyValueStore<String, SpecificAvroUser> kvStore;
    private ApplicationPropertyDAO appPropertyDao;
    private ConcurrentHashMap<String, AsyncResponse> _tMap = new ConcurrentHashMap<>();

    @Autowired
    public void setAppPropertyDao(ApplicationPropertyDAO appPropertyDao) {
        this.appPropertyDao = appPropertyDao;
    }

//    @Autowired
//    public void setConcurrentMap(ConcurrentHashMap<String, AsyncResponse> map) {
//        this._tMap = map;
//    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        if (appPropertyDao != null)
            this.kvStore = (KeyValueStore) context.getStateStore(appPropertyDao.getUserStateStore());
//        kvStore = (KeyValueStore) context.getStateStore("storage2");
    }

    @Override
    public void process(String uuid, String booleanValue) {
/*        AsyncResponse asyncResponse = this._tMap.get(uuid);
        SpecificAvroUser user = kvStore.get(uuid);
        if (booleanValue.equalsIgnoreCase("true")) {
            asyncResponse.resume(Response.status(Response.Status.CREATED).entity(user).build());
        } else {
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(user).build());
        }
//        this.kvStore.put(k, v);
        logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: stored and forwarded: " + uuid + ": " + user.toString());*/

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
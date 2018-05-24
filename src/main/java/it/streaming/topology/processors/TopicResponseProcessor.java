package it.streaming.topology.processors;


import it.model.avro.ResponseDto;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import org.apache.avro.specific.SpecificRecord;
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
public class TopicResponseProcessor implements Processor<String, SpecificRecord> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ConcurrentHashMap<String, AsyncResponse> asyncMap = new ConcurrentHashMap<>();
    private ProcessorContext context;
    private KeyValueStore<String, SpecificRecord> responseStore;
    private KeyValueStore<String, SpecificRecord> requestStore;
    private ApplicationPropertyDAO appDao;
    private ConcurrentHashMap<String, SpecificRecord> specificMap = new ConcurrentHashMap<>();

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
        logger.info("TOPOLOGY# Response processor started");
        this.responseStore = (KeyValueStore) context.getStateStore(appDao.getResponseStateStore());
    }

    /**
     * legge da topic response <uuid, boolean>. legge da una mappa<uuid, asyncresponse>
     *
     * @param k
     * @param v
     */
    @Override
    public void process(String k, SpecificRecord v) {
        logger.info("TOPOLOGY# RESPONSE #MAP# specificMap size: " + specificMap.size());
        logger.info("TOPOLOGY# RESPONSE #MAP# asyncMap size: " + asyncMap.size());

/*        if (requestStore == null) {
            logger.error("TOPOLOGY# RESPONSE # STORE is null");
            return;
        }*/
        logger.info("TOPOLOGY# RESPONSE processing");
        ResponseDto response = (ResponseDto) v;
        AsyncResponse asyncResponse = this.asyncMap.get(k);
        context.forward(k, v);
        responseStore.put(k, v);
        if (asyncResponse == null) {
            logger.error("TOPOLOGY# RESPONSE # asyncResponse is null");
            return;
        }
        if (response.getValue()) {
            SpecificAvroUser user = (SpecificAvroUser) specificMap.get(k);
            if (user != null)
                asyncResponse.resume(Response.status(Response.Status.CREATED).entity(user.toString()).build());
            else {
                String error = "{\"message\":\"User is null\"}";
                asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(error).build());
                logger.error("TOPOLOGY# RESPONSE # user is null");
            }
        } else {
            String error = "{\"message\":\"Sorry, but the flag is set to false\"}";
            logger.info("TOPOLOGY# RESPONSE # flag is false");
            asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(error).build());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }

    public KeyValueStore<String, SpecificRecord> getResponseStore() {
        return responseStore;
    }

    public void setRequestStore(KeyValueStore<String, SpecificRecord> requestStore) {
        this.requestStore = requestStore;
    }

    public ConcurrentHashMap<String, AsyncResponse> getAsyncMap() {
        return this.asyncMap;
    }

    public void setAsyncMap(ConcurrentHashMap<String, AsyncResponse> asyncMap) {
        this.asyncMap = asyncMap;
    }

    public ConcurrentHashMap<String, SpecificRecord> getSpecificHashMap() {
        return this.specificMap;
    }
}
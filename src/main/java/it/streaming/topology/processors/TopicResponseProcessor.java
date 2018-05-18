package it.streaming.topology.processors;


import it.model.StateStoreWrapperSingleton;
import it.spring.ApplicationPropertyDAO;
import org.apache.avro.generic.GenericRecord;
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
public class TopicResponseProcessor implements Processor<String, GenericRecord> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;
    private KeyValueStore<String, GenericRecord> kvStore;
    private Random rand = new Random();
    private StateStoreWrapperSingleton storeWrapper;
    private ApplicationPropertyDAO appPropertyDao;
    private ConcurrentHashMap<String, AsyncResponse> _tMap;

    @Autowired
    public ApplicationPropertyDAO getAppPropertyDao() {
        return appPropertyDao;
    }

    @Autowired
    public void setConcurrentMap(ConcurrentHashMap<String, AsyncResponse> map) {
        this._tMap = map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        if (appPropertyDao != null)
            context.getStateStore(appPropertyDao.getStateStore1());
//        kvStore = (KeyValueStore) context.getStateStore("storage2");
    }

    @Override
    public void process(String k, GenericRecord v) {
        boolean b = rand.nextBoolean();
        this.kvStore.put(k, v);
        context.forward(k, v);
        logger.info("UserProcessor#process: stored and forwarded" + v.toString());
    }

    @Override
    public void punctuate(long timestamp) {
/*        this.context.schedule(1000, PunctuationType.STREAM_TIME, (timestamp) -> {
            KeyValueIterator<String, Long> iter = this.kvStore.all();
            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                context.forward(entry.key, entry.value.toString());
            }
            iter.close();

            // commit the current processing progress
            context.commit();
        });*/
        //deprecated
    }


    @Override
    public void close() {
        //kvStore.close();
    }

    public ProcessorContext getContext() {
        return this.context;
    }

    public KeyValueStore<String, GenericRecord> getKvStore() {
        return kvStore;
    }

    private void processMap(String uuid){
        AsyncResponse asyncResponse = this._tMap.get(uuid);
        asyncResponse.resume(Response.status(Response.Status.CREATED).entity(created).build());

    }
}
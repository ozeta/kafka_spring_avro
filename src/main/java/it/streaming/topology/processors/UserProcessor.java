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

import java.lang.invoke.MethodHandles;

@Component
public class UserProcessor implements Processor<String, GenericRecord> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;
    private KeyValueStore<String, GenericRecord> kvStore;
    private ApplicationPropertyDAO appDao;

    @Autowired
    public ApplicationPropertyDAO getAppPropertyDao() {
        return appDao;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore(appDao.getUserStateStore());
    }

    @Override
    public void process(String k, GenericRecord v) {
        logger.info("UserProcessor#process:" + v.toString());
        v.put("surname", v.get("surname") + "Processed");
        v.put("name", v.get("name") + "Processed");
        this.kvStore.put(k, v);
        context.forward(k, v);
        logger.info("UserProcessor#process: stored and forwarded" + v.toString());
    }

    @Override
    public void punctuate(long timestamp) {
        //deprecated
    }

    @Override
    public void close() {
        //kvStore.close();
    }
    public KeyValueStore<String, GenericRecord> getKvStore() {
        return kvStore;
    }
}
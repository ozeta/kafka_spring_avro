package it.streaming.topology.processors;


import it.model.StateStoreWrapperSingleton;
import it.spring.ApplicationPropertyDAO;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.invoke.MethodHandles;
import java.util.Random;

@Component
public class TopicRequestProcessor implements Processor<String, String> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;
    private Random rand = new Random();
    private StateStoreWrapperSingleton storeWrapper;


    private ApplicationPropertyDAO appPropertyDao;

    @Autowired
    public void setAppPropertyDao(ApplicationPropertyDAO appPropertyDao) {
        this.appPropertyDao = appPropertyDao;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        if (appPropertyDao != null)
            this.kvStore = (KeyValueStore) context.getStateStore(appPropertyDao.getRequestStateStore());
//        kvStore = (KeyValueStore) context.getStateStore("storage2");
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
    public void process(String k, String v) {
        Boolean b = rand.nextBoolean();
        this.kvStore.put(k, b.toString());
        context.forward(k, b.toString());
        logger.info("UserProcessor#TOPIC_REQUEST_PROCESSOR#: stored and forwarded: " + k + ": " + b.toString());
    }

    @Override
    public void punctuate(long timestamp) {

    }


    @Override
    public void close() {
    }

}
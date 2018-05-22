package it.streaming.topology.processors;

import it.model.avro.ResponseDto;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
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
public class NewProcessors {
    private ApplicationPropertyDAO appDao;

    @Autowired
    public void setAppDao(ApplicationPropertyDAO appDao) {
        this.appDao = appDao;
    }

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Component
    public class TopicRequestProcessor implements Processor<String, SpecificRecord> {
        private ConcurrentHashMap<String, SpecificRecord> specificUsersMap = new ConcurrentHashMap<>();

        private ProcessorContext context;
        private Random rand = new Random();1) ~[jersey-server-2.26.jar:na]
        at org.glassfish.jersey.server.model.internal.JavaResourceMethodDispatcherProvider$VoidOutInvoker.doDispatch(JavaResourceMethodDispatcherProvider.java:183) ~[jersey-server-2.26.jar:na]
        at org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.dispatch(AbstractJavaResourceMethodDispatcher.java:103) ~[jersey-server-2.26.jar:na]
        at org.glassfish.jersey.server.model.ResourceMethodInvoker.invoke(ResourceMethodInvoker.java:493) ~[jersey-server-2.26.jar:na]
        at org.glassfish.jersey.server.model.ResourceMethodInvoker.lambda$apply$0(ResourceMethodInvoker.java:405) ~[jersey-server-2.26.jar:na]
        at org.glassfish.jersey.server.ServerRuntime$AsyncResponder$2$1.run(ServerRuntime.java:843) ~[jersey-server-2.26.jar:na]
        at org.glassfish.jersey.internal.Errors$1.call(Errors.java:272) ~[jersey-common-2.26.jar:na]
        private KeyValueStore<String, SpecificRecord> store;

        @Override
        public void init(ProcessorContext processorContext) {
            context = processorContext;
            this.store = (KeyValueStore) context.getStateStore(appDao.getRequestStateStore());
            logger.info("TOPOLOGY# Request processor started");
            this.context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, this::punctate);
        }

        /**
         * salva lo specific avro user nella concurrent hashmap
         *
         * @param k
         * @param v
         */
        @Override
        public void process(String k, SpecificRecord v) {
            logger.info("TOPOLOGY# REQUEST processing");
            this.specificUsersMap.put(k, v);
            this.store.put(k,v);
        }

        /**
         * richiamato dallo scheduler, prende il dato dalla concurrent map.
         * genera il booleano e forwarda la nuova struttura <uuid, responsedto> nel sink.
         *
         * @param l
         */

        @Override
        public void punctuate(long l) {
        }

        private void punctate(long l) {
//            logger.info("TOPOLOGY# punctuating");
            if (this.specificUsersMap.size() == 0) {
//                logger.info("TOPOLOGY# REQUEST# #PUNCTUATE: NO NEW EVENTS");
                return;
            }
            this.specificUsersMap.forEach((k, v) -> {
                Boolean b = rand.nextBoolean();
                ResponseDto response = new ResponseDto();
                response.setUuid(k);
                response.setValue(b);
                context.forward(k, response);
                logger.info("TOPOLOGY# RESPONSE# #PUNCTUATE: forwarded: " + response.toString());
            });
            this.specificUsersMap.keySet().removeAll(this.specificUsersMap.keySet());
        }

        @Override
        public void close() {

        }
    }

    @Component
    public class TopicResponseProcessor implements Processor<String, SpecificRecord> {
        private ConcurrentHashMap<String, AsyncResponse> asyncMap = new ConcurrentHashMap<>();
        private ProcessorContext context;
        private KeyValueStore<String, SpecificRecord> store;

        @Override
        public void init(ProcessorContext processorContext) {
            context = processorContext;
            logger.info("TOPOLOGY# Response processor started");
            this.store = (KeyValueStore) context.getStateStore(appDao.getResponseStateStore());
        }

        /**
         * legge da topic response <uuid, boolean>. legge da una mappa<uuid, asyncresponse>
         *
         * @param k
         * @param v
         */
        @Override
        public void process(String k, SpecificRecord v) {
            logger.info("TOPOLOGY# RESPONSE processing");
            ResponseDto response = (ResponseDto) v;
            AsyncResponse asyncResponse = this.asyncMap.get(k);
            context.forward(k, v);

            if (asyncResponse == null) {
                logger.error("TOPOLOGY# RESPONSE # asyncResponse is null");
                return;
            }
            if (response.getValue()) {
                SpecificAvroUser user = (SpecificAvroUser) store.get(k);
                if (user != null)
                    asyncResponse.resume(Response.status(Response.Status.CREATED).entity(user).build());
                else logger.error("TOPOLOGY# RESPONSE # user is null");
            } else {
                String error = "flag is false";
                logger.error("TOPOLOGY# RESPONSE # flag is false");
                asyncResponse.resume(Response.status(Response.Status.BAD_REQUEST).entity(error).build());
            }
        }

        @Override
        public void punctuate(long l) {

        }

        @Override
        public void close() {

        }

        public KeyValueStore<String, SpecificRecord> getStore() {
            return store;
        }

        public ConcurrentHashMap<String, AsyncResponse> getConcurrentHashMap() {
            return asyncMap;
        }
    }
}

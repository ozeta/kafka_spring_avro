package it.controller;

import it.AvroUser;
import it.model.User;
import it.spring.ApplicationPropertyDAO;
import it.streaming.AvroConsumer;
import it.streaming.AvroProducer;
import it.streaming.topology.JerseyTopology;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@Component
@Path("/kafka")
public class KafkaController {

    Logger log = Logger.getLogger(KafkaController.class);
    String myRecordSchema = "{\"type\":\"record\"," +
            "\"name\":\"myrecord\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    @Autowired
    JerseyTopology jerseyTopology;
    private ApplicationPropertyDAO appPropDao;
    private JerseyTopology topology;

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                  final QueryableStoreType<T> queryableStoreType,
                                                  final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

    @Autowired
    public void setTopology(JerseyTopology topology) {
        this.topology = topology;
        this.topology.init();

    }

    @Autowired
    public void setApplicationProperties(ApplicationPropertyDAO appPropDao) {
        this.appPropDao = appPropDao;
    }

    @GET
    @Path("/store/{id}")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public User storeGet(@PathParam("id") String id) {
        StringBuilder message = new StringBuilder();
        KeyValueStore<String, GenericRecord> store = jerseyTopology.getUserProcessorKeyValueStore();
        log.info("UserProcessor# LOOP1");

        store.all().forEachRemaining(el -> {
            String elId = el.value.get("id").toString();
            log.info("UserProcessor# state store has found id: " + elId);
            if (elId.equals(id)) {
                log.info("UserProcessor# state store has found id: " + elId + "ID FOUND!!!!");
            }

        });
        log.info("UserProcessor# LOOP2");

        store.all().forEachRemaining(el -> {
            String elId = el.value.get("id").toString();
            log.info("UserProcessor# state store has found id: " + elId);
        });
        GenericRecord genericRecord = store.get(id);
        User user = null;
        if (genericRecord != null) {
            user = new User();
            String id1 = genericRecord.get("id").toString();
            String name = genericRecord.get("name").toString();
            String surname = genericRecord.get("surname").toString();
            user.setId(id1);
            user.setName(name);
            user.setSurname(surname);
        }
        return user;
    }

    @GET
    @Path("/topology_store")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<User> topology() {
        StringBuilder message = new StringBuilder();
        KeyValueStore<String, GenericRecord> store = jerseyTopology.getUserProcessorKeyValueStore();
        LinkedList<JSONObject> l = new LinkedList<>();
        LinkedList<User> lUsers = new LinkedList<>();
        store.all().forEachRemaining(el -> {
            GenericRecord genericRecord = el.value;
            String id1 = genericRecord.get("id").toString();
            String name = genericRecord.get("name").toString();
            String surname = genericRecord.get("surname").toString();
            l.add(new JSONObject().put("id", id1).put("name", name).put("surname", surname));
            User user = new User();
            user.setId(id1);
            user.setName(name);
            user.setSurname(surname);
            lUsers.add(user);
            log.info("UserProcessor# state store key: " + el.key + "value: " + el.value);
            String tmp = "key: " + el.key + ", value: " + el.value + "\n";
            message.append(tmp);
        });
        return lUsers;
    }

    @GET
    @Path("/health/{id}")
    @Produces(MediaType.TEXT_PLAIN_VALUE)
    public String health(@PathParam("id") String id) {
        log.info("Service is up & running with id is: " + id + "!");
        return "Service is up & running and the id is: " + id + "!";
    }

    @POST
    @Path("/avro_producer")
    @Produces(MediaType.TEXT_PLAIN_VALUE)
    public String avroProducer(User user) {
        Future response = new AvroProducer<AvroUser, User>(appPropDao.getIp(), appPropDao.getPort(), appPropDao.getTopic())
//                .withGeneric()
                .withSpecific()
                .produce(user);
        try {
            if (response != null) {
                Object o = response.get();
                log.info("UserProcessor# Response: " + o.toString());
                return "avro processed";
            }
        } catch (Exception e) {
            log.error("UserProcessor# ERROR: ", e);
        }
        return "error";
    }


    @GET
    @Path("/avro_topology_consumer")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<?> avroTopologyConsumer() {
        return consumer(appPropDao.getTopologyTopic(), false);
    }

    @GET
    @Path("/avro_consumer")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<?> avroConsumer() {
        return consumer(appPropDao.getTopic(), false);
    }

    @GET
    @Path("/specific_avro_consumer")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<?> specificAvroConsumer() {
        return consumer(appPropDao.getTopic(), true);
    }

    private List<?> consumer(String _topic, Boolean specific) {
        Schema schema = new Schema.Parser().parse(myRecordSchema);

        List<User> consume;
        if (specific) {
            consume = new AvroConsumer<AvroUser, User>(appPropDao.getIp(), appPropDao.getPort(), _topic)
                    .withGeneric()
                    .consume();
        } else {
            consume = new AvroConsumer<GenericRecord, User>(appPropDao.getIp(), appPropDao.getPort(), _topic)
                    .withGeneric()
                    .consume();
        }


        StringBuilder message = new StringBuilder();
        List<User> collect = consume.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return collect;
    }

}

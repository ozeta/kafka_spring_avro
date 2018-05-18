package it.controller;

import it.model.User;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.AvroConsumer;
import it.streaming.AvroProducer;
import it.streaming.topology.AsyncTopology;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ManagedAsync;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Path("/kafka")
public class KafkaController {

    Logger log = Logger.getLogger(KafkaController.class);

    AsyncTopology asyncTopology;
    ConcurrentHashMap<String, AsyncResponse> _tMap;
    private ApplicationPropertyDAO appPropDao;
    private AsyncTopology topology;

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
    public void setTopology(AsyncTopology topology) {
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
        KeyValueStore<String, GenericRecord> store = asyncTopology.getUserProcessorKeyValueStore();
        log.info("UserProcessor# LOOP1");

        store.all().forEachRemaining(el -> {
            String elId = el.value.get("id").toString();
            log.info("UserProcessor# state store has found id: " + elId);
            if (elId.equals(id)) {
                log.info("UserProcessor# state store has found id: " + elId + "ID FOUND!!!!");
            }
        });
        log.info("UserProcessor# LOOP2");

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
        KeyValueStore<String, GenericRecord> store = asyncTopology.getUserProcessorKeyValueStore();
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
        Future response = new AvroProducer<String, SpecificAvroUser, User>(appPropDao.getIp(), appPropDao.getPort(), appPropDao.getTopic())
//                .withGenericSerializer()
                .withSpecificSerializer()
                .produce(user.getId(), user);
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

    @POST
    @ManagedAsync
    @Path("/async_post")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public void asyncPost(@Suspended final AsyncResponse asyncResponse, User user) {
        int timeout = 1000;
        asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
        asyncResponse.setTimeoutHandler(ar -> ar.resume(
                Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity("Operation timed out after " + timeout + " ms.")
                        .build()));
        String uuid = this.getUUID();
        _tMap = asyncTopology.getConcurrentHashMap();
        _tMap.put(uuid, asyncResponse);
        Future response = new AvroProducer<String, SpecificAvroUser, User>(appPropDao.getIp(), appPropDao.getPort(), appPropDao.getAsyncRequestTopic())
                .withSpecificSerializer()
                .produce(uuid, user);
/*        try {
            asyncResponse.resume(response.get().toString());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }*/
    }

    @GET
    @ManagedAsync
    @Path("async_get/{uuid}")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public void asyncGet(@PathParam("uuid") String id){
//        _tMap.get(id).resume(result);
    }

    private String getUUID() {
        UUID uuid = UUID.randomUUID();
        String digest = "";
        try {
        MessageDigest salt = MessageDigest.getInstance("SHA-256");
            salt.update(UUID.randomUUID().toString().getBytes("UTF-8"));
            digest = Hex.encodeHexString(salt.digest());
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            digest = String.valueOf(System.nanoTime());
            e.printStackTrace();
        }
        return digest;
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
        List<User> consume;
        if (specific) {
            consume = new AvroConsumer<SpecificAvroUser, User>(appPropDao.getIp(), appPropDao.getPort(), _topic)
                    .withSpecific()
                    .consume();
            log.info("UserProcessor# CONSUMED");
        } else {
            consume = new AvroConsumer<GenericRecord, User>(appPropDao.getIp(), appPropDao.getPort(), _topic)
                    .withGeneric()
                    .consume();
        }

        List<User> collect = consume.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        collect.forEach(o -> {
            log.info("UserProcessor# Response: " + o.toString());
        });

        return collect;
    }

}

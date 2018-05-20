package it.controller;

import it.model.User;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.AvroConsumer;
import it.streaming.AvroProducer;
import it.streaming.topology.AsyncTopology;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.streams.state.KeyValueStore;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Path("/kafka")
public class KafkaController {

    Logger log = Logger.getLogger(KafkaController.class);
    private AsyncTopology asyncTopology;
    private KeyValueStore<String, SpecificAvroUser> _store;
    private ConcurrentHashMap<String, AsyncResponse> _tMap;
    private ApplicationPropertyDAO appDao;

    @Autowired
    public void setAsyncTopology(AsyncTopology asyncTopology) {
        this.asyncTopology = asyncTopology;
        this.asyncTopology.init();

    }

    @Autowired
    public void setApplicationProperties(ApplicationPropertyDAO appPropDao) {
        this.appDao = appPropDao;
    }

    @GET
    @Path("/_store/{id}")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public User storeGet(@PathParam("id") String id) {
        StringBuilder message = new StringBuilder();
        KeyValueStore<String, SpecificAvroUser> store = asyncTopology.getUserProcessorKeyValueStore();
        log.info("UserProcessor# LOOP1");

        store.all().forEachRemaining(el -> {
            String elId = el.value.get("id").toString();
            log.info("UserProcessor# state _store has found id: " + elId);
            if (elId.equals(id)) {
                log.info("UserProcessor# state _store has found id: " + elId + "ID FOUND!!!!");
            }
        });
        log.info("UserProcessor# LOOP2");

        SpecificAvroUser genericRecord = store.get(id);
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
        KeyValueStore<String, SpecificAvroUser> store = asyncTopology.getUserProcessorKeyValueStore();
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
            log.info("UserProcessor# state _store key: " + el.key + "value: " + el.value);
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
        Future response = new AvroProducer<String, SpecificAvroUser, User>(appDao.getIp(), appDao.getPort(), appDao.getTopic())
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
        try {

            SpecificAvroUser specificAvroUser = new SpecificAvroUser();
            specificAvroUser.setId(user.getId());
            specificAvroUser.setName(user.getName());
            specificAvroUser.setSurname(user.getSurname());

            int timeout = 1000;

            asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
            asyncResponse.setTimeoutHandler(ar -> ar.resume(
                    Response.status(Response.Status.GATEWAY_TIMEOUT)
                            .entity("Operation timed out after " + timeout + " ms.")
                            .build()));
            String uuid = this.getUUID();


            Future response = new AvroProducer<String, SpecificAvroUser, User>(appDao.getIp(), appDao.getPort(), appDao.getAsyncRequestTopic())
                    .withSpecificSerializer()
                    .produce(uuid, user);
//            _store = asyncTopology.getUserProcessorKeyValueStore();
            _tMap = asyncTopology.getConcurrentHashMap();
//            _store.put(uuid, specificAvroUser);
            _tMap.put(uuid, asyncResponse);
/*        try {
            asyncResponse.resume(response.get().toString());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }*/
        } catch (Exception e) {
            log.error("UserProcessor#ERROR: ", e);
        }
    }

    @GET
    @ManagedAsync
    @Path("async_get/{uuid}")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public void asyncGet(@PathParam("uuid") String id) {
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
        return consumer(appDao.getTopologyTopic(), false);
    }

    @GET
    @Path("/avro_consumer")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<?> avroConsumer() {
        return consumer(appDao.getTopic(), false);
    }

    @GET
    @Path("/specific_avro_consumer")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<?> specificAvroConsumer() {
        return consumer(appDao.getTopic(), true);
    }

    private List<?> consumer(String _topic, Boolean specific) {
        List<User> consume;
        if (specific) {
            consume = new AvroConsumer<SpecificAvroUser, User>(appDao.getIp(), appDao.getPort(), _topic)
                    .withSpecific()
                    .consume();
            log.info("UserProcessor# CONSUMED");
        } else {
            consume = new AvroConsumer<GenericRecord, User>(appDao.getIp(), appDao.getPort(), _topic)
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

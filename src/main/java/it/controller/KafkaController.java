package it.controller;

import it.model.UserDTO;
import it.model.avro.SpecificAvroUser;
import it.spring.ApplicationPropertyDAO;
import it.streaming.AsyncSpecificProducer;
import it.streaming.topology.AsyncTopology2;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ManagedAsync;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
@Path("/kafka")
public class KafkaController {
    Logger log = Logger.getLogger(KafkaController.class);
    private KeyValueStore<String, SpecificRecord> _store;
    private ConcurrentHashMap<String, AsyncResponse> _asyncMap;
    private ConcurrentHashMap<String, SpecificRecord> _specificMap;

    private AsyncTopology2 asyncTopology;
    private ApplicationPropertyDAO appDao;
    private AsyncSpecificProducer asyncProducer;

    @Autowired
    public void setAsyncTopology(AsyncTopology2 asyncTopology) {
        this.asyncTopology = asyncTopology;
    }

    @Autowired
    public void setApplicationProperties(ApplicationPropertyDAO appPropDao) {
        this.appDao = appPropDao;
    }

    @Autowired
    public void setAsyncProducer(AsyncSpecificProducer asyncProducer) {
        this.asyncProducer = asyncProducer;
    }


    //region Description

/*

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
        List<UserDTO> consume;
        if (specific) {
            consume = new AvroConsumer<SpecificAvroUser, UserDTO>(appDao.getIp(), appDao.getPort(), _topic)
                    .withSpecific()
                    .consume();
            log.info("UserProcessor# CONSUMED");
        } else {
            consume = new AvroConsumer<GenericRecord, UserDTO>(appDao.getIp(), appDao.getPort(), _topic)
                    .withGeneric()
                    .consume();
        }

        List<UserDTO> collect = consume.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        collect.forEach(o -> {
            log.info("UserProcessor# Response: " + o.toString());
        });

        return collect;
    }
    @GET
    @Path("/_store/{id}")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public UserDTO storeGet(@PathParam("id") String id) {
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
        UserDTO user = null;
        if (genericRecord != null) {
            user = new UserDTO();
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
    public List<UserDTO> topology() {
        StringBuilder message = new StringBuilder();
        KeyValueStore<String, SpecificAvroUser> store = asyncTopology.getUserProcessorKeyValueStore();
        LinkedList<JSONObject> l = new LinkedList<>();
        LinkedList<UserDTO> lUsers = new LinkedList<>();
        store.all().forEachRemaining(el -> {
            GenericRecord genericRecord = el.value;
            String id1 = genericRecord.get("id").toString();
            String name = genericRecord.get("name").toString();
            String surname = genericRecord.get("surname").toString();
            l.add(new JSONObject().put("id", id1).put("name", name).put("surname", surname));
            UserDTO user = new UserDTO();
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
    public String avroProducer(UserDTO user) {
        Future response = new AvroProducer<String, SpecificAvroUser, UserDTO>(appDao.getIp(), appDao.getPort(), appDao.getTopic())
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
*/
    //endregion

    @POST
    @ManagedAsync
    @Path("/async_post")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public void asyncPost(@Suspended final AsyncResponse asyncResponse, UserDTO userDTO) {
        try {

            SpecificAvroUser specificAvroUser = userDTO.toAvro();

            Long timeout = appDao.getInitTimeout();

            asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
            asyncResponse.setTimeoutHandler(ar -> ar.resume(
                    Response.status(Response.Status.GATEWAY_TIMEOUT)
                            .entity("Operation timed out after " + timeout + " ms.")
                            .build()));
            String uuid = this.getUUID();

            Future response = asyncProducer.produce(uuid, specificAvroUser);
//            _store = asyncTopology.getUserProcessorKeyValueStore();
//            _store = asyncTopology.getUsersMap();
//            if (_store != null) _store.put(uuid, specificAvroUser);
            _asyncMap = asyncTopology.getResponseProcessorAsyncMap();
            if (_asyncMap != null) _asyncMap.put(uuid, asyncResponse);
            _specificMap = asyncTopology.getResponseProcessorSpecificMap();
            if (_specificMap != null) _specificMap.put(uuid, specificAvroUser);

            log.info("UserProcessor#DONE");
        } catch (Exception e) {
            log.error("UserProcessor#ERROR: ", e);
        }
    }


    /*
    @GET
    @ManagedAsync
    @Path("async_get/{uuid}")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public void asyncGet(@PathParam("uuid") String id) {
//        _asyncMap.get(id).resume(result);
    }
*/

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


}

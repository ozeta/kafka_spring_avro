package it.streaming;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import it.model.User;
import it.model.avro.AvroBuilder;
import it.model.avro.SpecificAvroUser;
import it.model.avro.GenericAvroBuilder;
import it.model.avro.SpecificAvroBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


public class AvroConsumer<P, C> {
    private String ip;
    private String topic;
    private String port;
    private AvroBuilder<P, C> recordBuilder;


    public AvroConsumer(String ip, String port, String topic) {
        this.ip = ip;
        this.port = port;
        this.topic = topic;
    }

    public List<C> consume() {
        if (this.recordBuilder == null) throw new RuntimeException("recordBuilder not configured");
        Logger log = Logger.getLogger(AvroConsumer.class);
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ip + ":" + port);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "group" + String
                        .valueOf(System.currentTimeMillis())
                        .substring(0, 5));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "http://" + ip + ":8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        final Consumer<String, P> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(this.topic));
        List<C> list = new LinkedList<>();

        long start = System.currentTimeMillis();
        long stop = System.currentTimeMillis();
        try {

            while ((stop - start) / 1000 < 2.5) {
                ConsumerRecords<String, P> records = consumer.poll(100);
                for (ConsumerRecord<String, P> record : records) {
                    C u = this.recordBuilder.consume(record);
                    if (u != null) list.add(u);
                    String message = "offset = " + record.offset() + " , key = " + record.key() + " , value = " + record.value();
                    log.info(message);
                }
                stop = System.currentTimeMillis();
            }

        } catch (Exception e) {
            log.error(e);
            log.error(e.getStackTrace());
        } finally {
            consumer.close();
        }
        return list;

    }

    public AvroConsumer<P, C> withGeneric() {
        this.recordBuilder = new GenericAvroBuilder<P, C>() {
            @Override
            public C consume(ConsumerRecord<String, P> record) {
                P value = record.value();
                if (!(value instanceof GenericRecord)) {
                    return null;
                }
                GenericRecord genericRecord = (GenericRecord) value;
                User u = new User();
                u.setId(genericRecord.get("id").toString());
                u.setName(genericRecord.get("name").toString());
                u.setSurname(genericRecord.get("surname").toString());
                return (C) u;
            }
        };
        return this;
    }

    public AvroConsumer<P, C> withSpecific() {
        this.recordBuilder = new SpecificAvroBuilder<P, C>() {
            @Override
            public C consume(ConsumerRecord<String, P> record) {
                P value = record.value();
                if (!(value instanceof SpecificAvroUser)) {
                    return null;
                }
                SpecificAvroUser specificAvroUser = (SpecificAvroUser) value;
                User u = new User();
                u.setId(specificAvroUser.getId().toString());
                u.setName(specificAvroUser.getName().toString());
                u.setSurname(specificAvroUser.getSurname().toString());
                return (C) u;
            }
        };
        return this;
    }

}

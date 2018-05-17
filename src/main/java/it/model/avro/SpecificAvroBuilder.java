package it.model.avro;

import it.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SpecificAvroBuilder<P, C> implements AvroBuilder<P, C> {

    /*    public Future send() {
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(this.topic, user.getId(), avroRecord);
            try {
                return this.producer.send(record);
            } catch (SerializationException e) {
                log.info(e.getMessage());
                return null;
            }
        }*/
    public P build(User user) {
        AvroUser avroUser = new AvroUser();
        String key = Long.toString(System.currentTimeMillis());
        key = user.getId() + key;
        avroUser.setId(key);
        avroUser.setName(user.getName());
        avroUser.setSurname(user.getSurname());
        return (P) avroUser;
    }

    @Override
    public C consume(ConsumerRecord<String, P> record) {
        return null;
    }

}
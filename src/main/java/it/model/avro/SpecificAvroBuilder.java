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
        SpecificAvroUser specificAvroUser = new SpecificAvroUser();
        String key = Long.toString(System.currentTimeMillis());
        key = user.getId() + key;
        specificAvroUser.setId(key);
        specificAvroUser.setName(user.getName());
        specificAvroUser.setSurname(user.getSurname());
        return (P) specificAvroUser;
    }

    @Override
    public C consume(ConsumerRecord<String, P> record) {
        return null;
    }

}
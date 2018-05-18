package it.model.avro;

import it.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public interface AvroBuilder<P,C> {
    P build(User user);
    C consume(ConsumerRecord<String, P> record);
}

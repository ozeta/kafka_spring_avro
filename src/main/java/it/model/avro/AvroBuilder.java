package it.model.avro;

import it.model.UserDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public interface AvroBuilder<P,C> {
    P build(UserDTO userDTO);
    C consume(ConsumerRecord<String, P> record);
}

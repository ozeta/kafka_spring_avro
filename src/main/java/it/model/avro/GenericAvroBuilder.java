package it.model.avro;

import it.model.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class GenericAvroBuilder<P, C> implements AvroBuilder<P, C> {

    @Override
    public P build(User user) {
        String userSchema = null;

        userSchema = "{\n" +
                "   \"namespace\": \"it\",\n" +
                "   \"doc\":\"User documentation\",\n" +
                "   \"type\": \"record\",\n" +
                "   \"name\": \"SpecificAvroUser\",\n" +
                "   \"fields\": [\n" +
                "      {\"name\": \"id\", \"type\": \"string\"},\n" +
                "      {\"name\": \"name\", \"type\": \"string\"},\n" +
                "      {\"name\": \"surname\", \"type\": \"string\"}\n" +
                "   ]\n" +
                "}\n";
        String key = Long.toString(System.currentTimeMillis());
        key = user.getId() + key;
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", key);
        avroRecord.put("name", user.getName());
        avroRecord.put("surname", user.getSurname());
        return (P) avroRecord;
    }

    @Override
    public C consume(ConsumerRecord<String, P> record) {
        return null;
    }

}


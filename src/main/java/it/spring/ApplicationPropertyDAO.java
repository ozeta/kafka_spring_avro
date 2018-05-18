package it.spring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ApplicationPropertyDAO {
    @Value("${kafka.ip}")
    private String ip;
    @Value("${kafka.topic}")
    private String topic;
    @Value("${kafka.topic.async.request}")
    String asyncRequestTopic;

    @Value("${kafka.topic.async.response}")
    String asyncResponseTopic;
    @Value("kafka.topology.topic")
    private String topologyTopic;
    @Value("${kafka.port}")
    private String port;
    @Value("${kafka.state_store1}")
    private String stateStore1;
    @Value("${kafka.state_store2}")
    private String stateStore2;
    @Value("${kafka.schema.registry.host}")
    private String schemaRegistryHost;
    @Value("${kafka.schema.registry.port}")
    private String schemaRegistryPort;

    public String getIp() {
        return ip;
    }

    public String getTopic() {
        return topic;
    }

    public String getTopologyTopic() {
        return topologyTopic;
    }

    public String getPort() {
        return port;
    }

    public String getStateStore1() {
        return stateStore1;
    }
    public String getStateStore2() {
        return stateStore2;
    }

    public String getSchemaRegistryHost() {
        return schemaRegistryHost;
    }

    public String getSchemaRegistryPort() {
        return schemaRegistryPort;
    }

    public String getAsyncRequestTopic() {
        return asyncRequestTopic;
    }

    public String getAsyncResponseTopic() {
        return asyncResponseTopic;
    }

}


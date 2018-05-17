package it.spring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ApplicationPropertyDAO {
    @Value("${kafka.ip}")
    private String ip;
    @Value("${kafka.topic}")
    private String topic;
    @Value("kafka.topology.topic")
    private String topologyTopic;
    @Value("${kafka.port}")
    private String port;
    @Value("${kafka.state_store}")
    private String stateStore;
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

    public String getStateStore() {
        return stateStore;
    }

    public String getSchemaRegistryHost() {
        return schemaRegistryHost;
    }

    public String getSchemaRegistryPort() {
        return schemaRegistryPort;
    }
}


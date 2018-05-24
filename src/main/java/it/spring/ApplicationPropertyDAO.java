package it.spring;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ApplicationPropertyDAO {
    @Value("${kafka.ip}")
    private String ip;
    @Value("${kafka.topic}")
    private String topic;
    @Value("${kafka.topic.async.request}")
    private String asyncRequestTopic;
    @Value("${kafka.topic.async.response}")
    private String asyncResponseTopic;
    @Value("${kafka.topic.async.response.sink}")
    private String asyncResponseSinkTopic;
    @Value("kafka.topology.topic")
    private String topologyTopic;
    @Value("${kafka.topic.port}")
    private String port;
    @Value("${kafka.state.store.user}")
    private String userStateStore;
    @Value("${kafka.state.store.request}")
    private String requestStateStore;
    @Value("${kafka.state.store.response}")
    private String responseStateStore;
    @Value("${kafka.schema.registry.host}")
    private String schemaRegistryHost;
    @Value("${kafka.schema.registry.port}")
    private String schemaRegistryPort;
    @Value("${kafka.processor.timeout}")
    private long timeout;

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

    public String getRequestStateStore() {
        return requestStateStore;
    }

    public String getResponseStateStore() {
        return responseStateStore;
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

    public String getUserStateStore() {
        return userStateStore;
    }

    public long getInitTimeout() {
        return timeout;
    }

    public String getAsyncResponseSinkTopic() {
        return asyncResponseSinkTopic;
    }
}


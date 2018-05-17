package it.model;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class StateStoreWrapperSingleton {
    private static StateStoreWrapperSingleton instance = null;
    private KeyValueStore<String, GenericRecord> kvStore;

    public ProcessorContext getContext() {
        return context;
    }

    private ProcessorContext context;

    private StateStoreWrapperSingleton() {

    }

    public static synchronized StateStoreWrapperSingleton getInstance() {
        if (instance == null) {
            instance = new StateStoreWrapperSingleton();
        }
        return instance;
    }

    public KeyValueStore<String, GenericRecord> getKvStore() {
        return kvStore;
    }

    public void setKvStore(KeyValueStore<String, GenericRecord> kvStore) {
        this.kvStore = kvStore;
    }

    public void setProcessorContext(ProcessorContext context) {
        this.context = context;
    }
}

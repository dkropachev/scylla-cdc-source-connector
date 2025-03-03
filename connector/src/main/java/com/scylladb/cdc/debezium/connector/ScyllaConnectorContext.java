package com.scylladb.cdc.debezium.connector;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

public interface ScyllaConnectorContext extends OffsetStorageReader, ConnectorContext {}

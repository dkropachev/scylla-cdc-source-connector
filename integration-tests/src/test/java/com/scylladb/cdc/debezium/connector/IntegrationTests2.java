package com.scylladb.cdc.debezium.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.scylladb.cdc.debezium.connector.infra.ccm.CcmBridge;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.net.InetSocketAddress;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntegrationTests2 {
  CcmBridge CCM_BRIDGE;

  @BeforeAll
  void setupOnce() {
    CCM_BRIDGE = CcmBridge.builder()
        .withNodes(3).build();
    CCM_BRIDGE.create();
    CCM_BRIDGE.start();
  }

  @AfterAll
  void destroyOnce() {
    if (CCM_BRIDGE != null) CCM_BRIDGE.remove();
  }

  @Test
  public void should_execute_synchronously() {
    String keyspace = "test_keyspace";
    String table = "test_table";
//    props.setProperty("database.password", "password");


    try (CqlSession session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(CCM_BRIDGE.getNodeIpAddress(1), 9042))
        .withLocalDatacenter("dc1")
        .build()) {

      System.out.println("Connected to ScyllaDB");

      session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s ", keyspace) +
          "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");

      session.execute(String.format("CREATE TABLE %s.%s (pk int, v int, PRIMARY KEY (pk)) WITH cdc = {'enabled':true};", keyspace, table));

      ThreadLocalRandom rnd = ThreadLocalRandom.current();

      for (int i = 0; i < 1000; i++) {
        session.execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, table), rnd.nextInt(), rnd.nextInt());
      }

      Map<String, String> props = new HashMap<>();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
      props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
      props.put("offset.storage.file.filename", "/tmp/connect.offsets");
      props.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
      props.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
      props.put("worker.id", "embedded-connect");
      // ... other required props

      // 2) Create a StandaloneConfig
      StandaloneConfig config = new StandaloneConfig(props);

      // 3) Initialize worker
      String workerId = "embedded";
      Worker worker = new Worker(workerId, new SystemTime(), new Plugins(props), config, new FileOffsetBackingStore(), null);

      // 4) Create Herder
      Herder herder = new StandaloneHerder(worker, "embedded-kafka-cluster", null);

      // 5) Create Connect
      Connect connect = new Connect(herder, new RestServer(config));

      // 6) Start Connect
      connect.start();

      Map<String, String> connectorProps = new HashMap<>();
      connectorProps.put("name", "my-source-connector");
      connectorProps.put("connector.class", "com.scylladb.cdc.debezium.connector.ScyllaConnector");
      connectorProps.put("tasks.max", "1");
      connectorProps.put("topic", "test-topic");

      connectorProps.put("scylla.name", "Test Cluster");
      connectorProps.put("scylla.cluster.ip.addresses", CCM_BRIDGE.getNodeIpAddress(1) + ":9042");
      connectorProps.put("scylla.table.names", keyspace + "." + table);
      connectorProps.put("scylla.local.dc", "dc1");
      connectorProps.put("offset.storage.file.filename", "/path/to/offsets.dat");

      // ... your custom connector settings

      // Register the connector
      herder.putConnectorConfig("com.scylladb.cdc.debezium.connector.ScyllaConnector", connectorProps, false, (error, result) -> {
        if (error != null) {
          error.printStackTrace();
        } else {
          System.out.println("Connector started: " + result.toString());
        }
      });

      connect.stop();
    }
  }
}

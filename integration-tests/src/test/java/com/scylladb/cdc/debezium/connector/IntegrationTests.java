package com.scylladb.cdc.debezium.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.scylladb.cdc.debezium.connector.infra.ccm.CcmBridge;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntegrationTests {
  CcmBridge CCM_BRIDGE;

  @BeforeAll
  void setupOnce() {
//    CCM_BRIDGE = CcmBridge.builder()
//        .withNodes(3).build();
//    CCM_BRIDGE.create();
//    CCM_BRIDGE.start();
  }

  @AfterAll
//  void destroyOnce() {
//    if (CCM_BRIDGE != null) CCM_BRIDGE.remove();
//  }

  @Test
  public void should_execute_synchronously() {
    String keyspace = "test_keyspace";
    String table = "test_table";
//    props.setProperty("database.password", "password");

//    String firstNode = CCM_BRIDGE.getNodeIpAddress(1);
//    String datacenter = "dc1";
    String firstNode = "172.17.0.2";
    String datacenter = "datacenter1";


    try (CqlSession session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(firstNode, 9042))
//        .withLocalDatacenter("dc1")
        .withLocalDatacenter(datacenter)
        .build()) {

      System.out.println("Connected to ScyllaDB");

      session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s ", keyspace) +
          "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

      session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (pk int, v int, PRIMARY KEY (pk)) WITH cdc = {'enabled':true};", keyspace, table));

      ThreadLocalRandom rnd = ThreadLocalRandom.current();

      for (int i = 0; i < 1000; i++) {
        session.execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, table), rnd.nextInt(), rnd.nextInt());
      }

      Properties props = new Properties();
      // Unique name for the connector
      props.setProperty("name", "embedded-mysql-connector");
      // Connector class
      props.setProperty("connector.class", "com.scylladb.cdc.debezium.connector.ScyllaConnector");

      // Database connection configuration
      props.setProperty("scylla.name", "Test Cluster");
      props.setProperty("scylla.cluster.ip.addresses", firstNode + ":9042");
      props.setProperty("scylla.table.names", keyspace + "." + table);
      props.setProperty("scylla.local.dc", datacenter);
      props.setProperty("offset.storage.file.filename", "/path/to/offsets.dat");
//      props.setProperty("database.history.file.filename", "/path/to/dbhistory.dat");
//      props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");

      // OPTIONAL: Set time between connection checks, heartbeat intervals, etc.
//    props.setProperty("connect.timeout.ms", "30000");
//    props.setProperty("connect.keep.alive", "true");

      // 2) Build the Debezium Engine with JSON output
      DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
          .using(props) // pass the configuration
          .notifying(new DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>>() {
            @Override
            public void handleBatch(List<ChangeEvent<String, String>> list, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter) throws InterruptedException {
              for (ChangeEvent<String, String> record : list) {
                String key = record.key();
                String value = record.value();
                System.out.println("Received event key = " + key + ", value = " + value);
              }
            }
          })
          .build();

      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.execute(engine);
      try {
        Thread.sleep(10000000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          engine.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}

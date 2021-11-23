/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.jdbc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.storage.StorageConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class JDBCStorageManagerTest {
    private static final String SHARED_CONNECTION_URL = "jdbc:sqlite:file:test?mode=memory&cache=shared";
    private static final String TEMP_CONNECTION_URL = "jdbc:sqlite::memory:";
    private JDBCStorageManager<String> memoryManager;

    private static JDBCStorageManager<String> getStorageManager(String url, int partitionCount) {
        JDBCConfig config = new JDBCConfig(new BulletConfig());
        config.set(JDBCConfig.JDBC_DB_CONNECTION_URL, url);
        config.set(StorageConfig.PARTITION_COUNT, partitionCount);
        config.validate();
        return new JDBCStorageManager<>(config);
    }

    @BeforeClass
    public void setup() {
        memoryManager = getStorageManager(SHARED_CONNECTION_URL, 10);
    }

    @Test
    public void testLiquibaseSetup() throws Exception {
        final String connectionUrl = "jdbc:sqlite:file:testLiquibaseSetup?mode=memory&cache=shared";

        JDBCStorageManager<String> manager = getStorageManager(connectionUrl, 10);

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT ID, AUTHOR, FILENAME FROM DATABASECHANGELOG")) {
            Assert.assertEquals(resultSet.getString(1), "schema");
            Assert.assertEquals(resultSet.getString(2), "bullet");
            Assert.assertEquals(resultSet.getString(3), "liquibase/database-changelog-schema.xml");
        }

        manager.close();
    }

    @Test
    public void testStoringPartitionCount() throws Exception {
        final String connectionUrl = "jdbc:sqlite:file:testStoringPartitionCount?mode=memory&cache=shared";

        JDBCStorageManager<String> manager = getStorageManager(connectionUrl, 10);

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT partition_count FROM constants")) {
            Assert.assertEquals(resultSet.getInt(1), 10);
        }

        manager.close();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*partition count.*")
    public void testMismatchedPartitionCount() {
        getStorageManager(SHARED_CONNECTION_URL, 20);
    }

    @Test
    public void testPutGetRemove() throws Exception {
        final String connectionUrl = "jdbc:sqlite:file:testPutGetRemove?mode=memory&cache=shared";

        JDBCStorageManager<String> manager = getStorageManager(connectionUrl, 1);

        Assert.assertTrue(manager.put("key", "foo").get());

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT query, partition FROM queries WHERE id = 'key'")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(SerializerDeserializer.fromBytes(resultSet.getBytes(1)), "foo");
            Assert.assertEquals(resultSet.getInt(2), 0);
        }

        Assert.assertEquals(manager.get("key").get(), "foo");
        Assert.assertEquals(manager.remove("key").get(), "foo");

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT query, partition FROM queries WHERE id = 'key'")) {
            Assert.assertFalse(resultSet.next());
        }

        manager.close();
    }

    @Test
    public void testPutAllGetAllClear() throws Exception {
        final String connectionUrl = "jdbc:sqlite:file:testPutAllGetAllClear?mode=memory&cache=shared";

        JDBCStorageManager<String> manager = getStorageManager(connectionUrl, 10);

        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            data.put(String.valueOf(i), null);
        }

        Assert.assertTrue(manager.putAll(data).get());

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT id, query FROM queries")) {
            for (int i = 0; i < 10; i++) {
                Assert.assertTrue(resultSet.next());
                Assert.assertTrue(data.containsKey(resultSet.getString(1)));
                Assert.assertNull(resultSet.getBytes(2));
            }
            Assert.assertFalse(resultSet.next());
        }

        Assert.assertEquals(manager.getAll().get(), data);
        Assert.assertEquals(manager.getAll(data.keySet()).get(), data);
        Assert.assertTrue(manager.clear(data.keySet()).get());

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT id, query FROM queries")) {
            Assert.assertFalse(resultSet.next());
        }

        manager.close();
    }

    @Test
    public void testPutException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager(TEMP_CONNECTION_URL, 10);
        manager.close();
        Assert.assertFalse(manager.put("key", "foo").get());
    }

    @Test
    public void testPutAllException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager(TEMP_CONNECTION_URL, 10);
        manager.close();

        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            data.put(String.valueOf(i), null);
        }

        Assert.assertFalse(manager.putAll(data).get());
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Failed to get connection from data source.*")
    public void testGetException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager(TEMP_CONNECTION_URL, 10);
        manager.close();
        manager.get("key").get();
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Failed to get connection from data source.*")
    public void testGetAllException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager(TEMP_CONNECTION_URL, 10);
        manager.close();

        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ids.add(String.valueOf(i));
        }

        manager.getAll(ids).get();
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Failed to get connection from data source.*")
    public void testRemoveException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager(TEMP_CONNECTION_URL, 10);
        manager.close();
        manager.remove("key").get();
    }

    @Test
    public void testClearSetException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager(TEMP_CONNECTION_URL, 10);
        manager.close();

        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            ids.add(String.valueOf(i));
        }

        Assert.assertFalse(manager.clear(ids).get());
    }

    @Test
    public void testGetAll() throws Exception {
        // hmm.. batch operations..?
    }

    @Test
    public void testNumberOfPartitions() {
        Assert.assertEquals(memoryManager.numberOfPartitions(), 10);
    }

    @Test
    public void testGetPartition() throws Exception {
        final String connectionUrl = "jdbc:sqlite:file:temp?mode=memory&cache=shared";

        JDBCStorageManager<String> manager = getStorageManager(connectionUrl, 10);

        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            data.put(String.valueOf(i), null);
        }

        Assert.assertTrue(manager.putAll(data).get());

        Set<String> ids = new HashSet<>();

        for (int i = 0; i < 10; i++) {
            Map<String, String> partition = manager.getPartition(i).get();
            Set<String> keys = new HashSet<>();
            try (Connection conn = DriverManager.getConnection(connectionUrl);
                 Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT id FROM queries WHERE partition = " + i)) {
                while (resultSet.next()) {
                    keys.add(resultSet.getString(1));
                }
                Assert.assertEquals(partition.keySet(), keys);
                ids.addAll(keys);
            }
        }

        Assert.assertEquals(ids, data.keySet());

        manager.close();
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*Failed to get connection from data source.*")
    public void testGetPartitionException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager("jdbc:sqlite::memory:", 10);
        manager.close();
        manager.getPartition(0).get();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Partition.*out of bounds.*")
    public void testGetPartitionOutOfBounds() throws Exception {
        JDBCStorageManager<String> manager = getStorageManager("jdbc:sqlite::memory:", 10);
        manager.close();
        manager.getPartition(10).get();
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Partition.*out of bounds.*")
    public void testGetPartitionOutOfBoundsTooSmall() throws Exception {
        JDBCStorageManager<String> manager = getStorageManager("jdbc:sqlite::memory:", 10);
        manager.close();
        manager.getPartition(-1).get();
    }

    @Test
    public void testClear() throws Exception {
        final String connectionUrl = "jdbc:sqlite:file:temp?mode=memory&cache=shared";

        JDBCStorageManager<String> manager = getStorageManager(connectionUrl, 10);

        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            data.put(String.valueOf(i), null);
        }

        Assert.assertTrue(manager.putAll(data).get());

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM queries")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getInt(1), 100);
        }

        Assert.assertTrue(manager.clear().get());

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM queries")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getInt(1), 0);
        }

        manager.close();
    }

    @Test
    public void testClearException() throws Exception {
        // coverage
        JDBCStorageManager<String> manager = getStorageManager("jdbc:sqlite::memory:", 10);
        manager.close();
        Assert.assertFalse(manager.clear().get());
    }
}

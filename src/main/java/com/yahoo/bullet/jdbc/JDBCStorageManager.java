/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.jdbc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.storage.StorageConfig;
import com.yahoo.bullet.storage.StorageManager;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class JDBCStorageManager<V extends Serializable> extends StorageManager<V> implements Serializable {
    private static final long serialVersionUID = -4767730072525278837L;

    private HikariDataSource dataSource;
    private final String tableName;
    private final int partitionCount;
    private final int batchSize;

    public JDBCStorageManager(BulletConfig config) {
        this(new JDBCConfig(config));
    }

    public JDBCStorageManager(JDBCConfig config) {
        super(config);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getRequiredConfigAs(JDBCConfig.JDBC_DB_CONNECTION_URL, String.class));
        hikariConfig.setUsername(config.getAs(JDBCConfig.JDBC_DB_USERNAME, String.class));
        hikariConfig.setPassword(config.getAs(JDBCConfig.JDBC_DB_PASSWORD, String.class));

        Map<String, Object> properties = config.getAllWithPrefix(Optional.empty(), JDBCConfig.JDBC_HIKARI_CP_PREFIX, true);
        properties.forEach(hikariConfig::addDataSourceProperty);

        dataSource = new HikariDataSource(hikariConfig);
        tableName = config.getRequiredConfigAs(JDBCConfig.JDBC_DB_TABLE_NAME, String.class);
        partitionCount = config.getRequiredConfigAs(StorageConfig.PARTITION_COUNT, Number.class).intValue();
        batchSize = config.getRequiredConfigAs(JDBCConfig.JDBC_BATCH_SIZE, Number.class).intValue();

        try (Connection conn = getConnection()) {
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn));
            Liquibase liquibase = new Liquibase("database-changelog-master.xml", new ClassLoaderResourceAccessor(), database);
            liquibase.update(new Contexts());
        } catch (Exception e) {
            log.error("Couldn't load Liquibase.", e);
            throw new RuntimeException(e);
        }

        setupPartitionCount();
    }

    @Override
    protected CompletableFuture<Boolean> putRaw(String namespace, String id, byte[] value) {
        log.debug("Setting {} to {} in putRaw", id, value);
        final String sql = "INSERT INTO '" + namespace + "' (id, query, partition) VALUES (?, ?, ?)";
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, id);
                statement.setBytes(2, value);
                statement.setInt(3, hash(id, partitionCount));
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        })
                .thenApply(JDBCStorageManager::onWriteSuccess)
                .exceptionally(JDBCStorageManager::onWriteFail);
    }

    @Override
    protected CompletableFuture<Boolean> putAllRaw(String namespace, Map<String, byte[]> data) {
        final String sql = "INSERT INTO '" + namespace + "' (id, query, partition) VALUES (?, ?, ?)";
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                Iterator<Map.Entry<String, byte[]>> iterator = data.entrySet().iterator();
                int currentBatchSize = 0;
                while (iterator.hasNext()) {
                    Map.Entry<String, byte[]> entry = iterator.next();
                    statement.setString(1, entry.getKey());
                    statement.setBytes(2, entry.getValue());
                    statement.setInt(3, hash(entry.getKey(), partitionCount));
                    statement.addBatch();
                    currentBatchSize += 1;
                    if (currentBatchSize >= batchSize || !iterator.hasNext()) {
                        statement.executeBatch();
                        currentBatchSize = 0;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        })
                .thenApply(JDBCStorageManager::onWriteSuccess)
                .exceptionally(JDBCStorageManager::onWriteFail);
    }

    @Override
    protected CompletableFuture<byte[]> getRaw(String namespace, String id) {
        log.debug("Getting {} in getRaw", id);
        final String sql = "SELECT query FROM '" + namespace + "' WHERE id = ?";
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, id);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.getBytes(1);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        })
                .thenApply(JDBCStorageManager::onReadSuccess)
                .exceptionally(JDBCStorageManager::onReadFail);
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace) {
        final String sql = "SELECT id, query FROM '" + namespace + "'";
        return CompletableFuture.supplyAsync(() -> {
            Map<String, byte[]> result = new HashMap<>();
            try (Connection conn = getConnection();
                 Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    result.put(resultSet.getString(1), resultSet.getBytes(2));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return result;
        })
                .thenApply(JDBCStorageManager::onReadSuccess)
                .exceptionally(JDBCStorageManager::onReadFail);
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace, Set<String> ids) {
        final String sql = "SELECT query FROM '" + namespace + "' WHERE id = ?";
        return CompletableFuture.supplyAsync(() -> {
            Map<String, byte[]> result = new HashMap<>();
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                for (String id : ids) {
                    statement.setString(1, id);
                    ResultSet resultSet = statement.executeQuery();
                    result.put(id, resultSet.getBytes(1));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return result;
        })
                .thenApply(JDBCStorageManager::onReadSuccess)
                .exceptionally(JDBCStorageManager::onReadFail);
    }

    @Override
    protected CompletableFuture<byte[]> removeRaw(String namespace, String id) {
        log.debug("Retrieving {} from redis before removing", id);
        return getRaw(namespace, id).thenComposeAsync(value -> removeAfterRead(namespace, id, value));
    }

    private CompletableFuture<byte[]> removeAfterRead(String namespace, String id, byte[] data) {
        log.debug("Removing {} from storage after retrieval {}", id, data);
        final String sql = "DELETE FROM '" + namespace + "' WHERE id = ?";
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, id);
                return statement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        })
                .thenApply(JDBCStorageManager::onRemoveSuccess)
                .thenApply(r -> data)
                .exceptionally(JDBCStorageManager::onRemoveFail);
    }

    @Override
    public CompletableFuture<Boolean> wipe() {
        return clear(getDefaultNamespace());
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace) {
        final String sql = "DELETE FROM '" + namespace + "'";
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = getConnection();
                 Statement statement = conn.createStatement()) {
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        })
                .thenApply(JDBCStorageManager::onClearSuccess)
                .exceptionally(JDBCStorageManager::onClearFail);
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace, Set<String> ids) {
        final String sql = "DELETE FROM '" + namespace + "' WHERE id = ?";
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                Iterator<String> iterator = ids.iterator();
                int currentBatchSize = 0;
                while (iterator.hasNext()) {
                    statement.setString(1, iterator.next());
                    statement.addBatch();
                    currentBatchSize += 1;
                    if (currentBatchSize >= batchSize || !iterator.hasNext()) {
                        statement.executeBatch();
                        currentBatchSize = 0;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        })
                .thenApply(JDBCStorageManager::onClearSetSuccess)
                .exceptionally(JDBCStorageManager::onClearSetFail);
    }

    @Override
    public void close() {
        super.close();
        dataSource.close();
    }

    @Override
    public int numberOfPartitions(String namespace) {
        return partitionCount;
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getPartitionRaw(String namespace, int partition) {
        if (partition < 0 || numberOfPartitions() <= partition) {
            throw new RuntimeException("Partition " + partition + " is out of bounds. Number of partitions: " + numberOfPartitions());
        }
        final String sql = "SELECT id, query FROM '" + namespace + "' WHERE partition = ?";
        return CompletableFuture.supplyAsync(() -> {
            Map<String, byte[]> result = new HashMap<>();
            try (Connection conn = getConnection();
                 PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, partition);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        result.put(resultSet.getString(1), resultSet.getBytes(2));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return result;
        })
                .thenApply(JDBCStorageManager::onReadSuccess)
                .exceptionally(JDBCStorageManager::onReadFail);
    }

    @Override
    protected String getDefaultNamespace() {
        return tableName;
    }

    private Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            log.debug("Failed to get connection from data source.", e);
            throw new RuntimeException("Failed to get connection from data source.", e);
        }
    }

    private void setupPartitionCount() {
        Integer existingCount = getPartitionCount();
        if (existingCount != null && existingCount != partitionCount) {
            log.error("Table {}, Partitions {} != {} ", tableName, existingCount, partitionCount);
            throw new RuntimeException("Table uses partition count that is not the same as the configured value");
        }
        storePartitionCount();
    }

    private Integer getPartitionCount() {
        final String sql = "SELECT partition_count FROM constants;";
        try (Connection conn = getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return (Integer) resultSet.getObject(1);
        } catch (SQLException e) {
            log.error("Couldn't get partition count.", e);
            throw new RuntimeException(e);
        }
    }

    private void storePartitionCount() {
        final String sql = "UPDATE constants SET partition_count = ?";
        try (Connection conn = getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, partitionCount);
            int result = statement.executeUpdate();
            log.info("Result of storing hash count: {}", result);
        } catch (SQLException e) {
            log.error("Couldn't store partition count.", e);
            throw new RuntimeException(e);
        }
    }

    private static <T> T onReadSuccess(T e) {
        log.debug("Succeeded in reading from storage", e);
        return e;
    }

    private static <T> boolean onWriteSuccess(T result) {
        log.debug("Succeeded in writing to storage. Received result {}", result);
        return true;
    }

    private static int onRemoveSuccess(int result) {
        log.debug("Succeeded in removing from storage. Removed rows {}", result);
        return result;
    }

    private static boolean onClearSuccess(Void unused) {
        log.debug("Succeeded in removing all rows from storage");
        return true;
    }

    private static boolean onClearSetSuccess(Void unused) {
        log.debug("Succeeded in removing specified rows from storage");
        return true;
    }

    private static <T> T onReadFail(Throwable e) {
        log.error("Failed in reading from storage", e);
        throw new RuntimeException(e);
    }

    private static boolean onWriteFail(Throwable e) {
        log.error("Failed in writing to storage", e);
        return false;
    }

    private static <T> T onRemoveFail(Throwable e) {
        log.error("Failed in removing from storage", e);
        throw new RuntimeException(e);
    }

    private static boolean onClearFail(Throwable e) {
        log.error("Failed in removing all rows from storage", e);
        return false;
    }

    private static boolean onClearSetFail(Throwable e) {
        log.error("Failed in removing specified rows from storage", e);
        return false;
    }
}

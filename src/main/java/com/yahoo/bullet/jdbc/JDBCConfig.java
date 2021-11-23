/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.jdbc;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import com.yahoo.bullet.storage.StorageConfig;

public class JDBCConfig extends StorageConfig {
    public static final String JDBC_DB_CONNECTION_URL = "bullet.storage.jdbc.db.connection.url";
    public static final String JDBC_DB_USERNAME = "bullet.storage.jdbc.db.username";
    public static final String JDBC_DB_PASSWORD = "bullet.storage.jdbc.db.password";
    public static final String JDBC_DB_TABLE_NAME = "bullet.storage.jdbc.db.table.name";
    public static final String JDBC_BATCH_SIZE = "bullet.storage.jdbc.batch.size";
    public static final String JDBC_HIKARI_CP_PREFIX = "bullet.storage.jdbc.hikari";

    // Defaults
    public static final String DEFAULT_CONFIGURATION_FILE = "bullet_jdbc_defaults.yaml";

    public static final String DEFAULT_JDBC_DB_TABLE_NAME = "queries";
    public static final int DEFAULT_JDBC_BATCH_SIZE = 10000;

    private static final Validator VALIDATOR = BulletConfig.getValidator();

    static {
        VALIDATOR.define(JDBC_DB_TABLE_NAME)
                 .checkIf(Validator::isString)
                 .defaultTo(DEFAULT_JDBC_DB_TABLE_NAME);
        VALIDATOR.define(JDBC_BATCH_SIZE)
                 .checkIf(Validator::isPositiveInt)
                 .defaultTo(DEFAULT_JDBC_BATCH_SIZE);
    }

    public JDBCConfig(String file) {
        this(new Config(file));
    }

    public JDBCConfig(Config other) {
        super(DEFAULT_CONFIGURATION_FILE);
        merge(other);
    }

    @Override
    public StorageConfig validate() {
        super.validate();
        VALIDATOR.validate(this);
        return this;
    }
}

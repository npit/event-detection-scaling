/*
 * Copyright 2015 SciFY NPO <info@scify.org>.
 *
 * This product is part of the NewSum Free Software.
 * For more information about NewSum visit
 *
 * 	http://www.scify.gr/site/en/projects/completed/newsum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * If this code or its output is used, extended, re-engineered, integrated,
 * or embedded to any extent in another software or hardware, there MUST be
 * an explicit attribution to this work in the resulting source code,
 * the packaging (where such packaging exists), or user interface
 * (where such an interface exists).
 *
 * The attribution must be of the form "Powered by NewSum, SciFY"
 *
 */
package gr.demokritos.iit.crawlers.twitter.repository.pool;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import gr.demokritos.iit.crawlers.twitter.factory.Configuration;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public class CDataSource {

    private static CDataSource datasource;
    private ComboPooledDataSource cpds;

    private CDataSource()
            throws IOException, SQLException, PropertyVetoException {

        Configuration config = new Configuration("./twitter.properties");

        this.cpds = new ComboPooledDataSource();
        this.cpds.setDriverClass("com.mysql.jdbc.Driver");
        this.cpds.setJdbcUrl("jdbc:" + config.getDatabaseHost());
        this.cpds.setUser(config.getDatabaseUserName());
        this.cpds.setPassword(config.getDatabasePassword());

        // the settings below are optional -- c3p0 can work with defaults
        this.cpds.setMinPoolSize(5);
        this.cpds.setAcquireIncrement(5);
        this.cpds.setMaxPoolSize(20);
        this.cpds.setMaxStatements(180);

    }

    public synchronized static CDataSource getInstance()
            throws IOException, SQLException, PropertyVetoException {

        if (datasource == null) {
            datasource = new CDataSource();
        }
        return datasource;

    }

    public Connection getConnection() throws SQLException {
        return cpds.getConnection();
    }
}

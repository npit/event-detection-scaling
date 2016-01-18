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
package gr.demokritos.iit.crawlers.twitter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import gr.demokritos.iit.crawlers.twitter.factory.Configuration;
import gr.demokritos.iit.crawlers.twitter.repository.CassandraRepository;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.repository.MySQLRepository;
import gr.demokritos.iit.crawlers.twitter.repository.pool.CDataSource;
import gr.demokritos.iit.crawlers.twitter.url.URLUnshortener;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public class CrawlScript {

    public static void main(String[] args) throws IOException, SQLException, PropertyVetoException {
        monitor(); // TODO get monitor/search from params
    }

    public static void monitor() throws IOException, SQLException, PropertyVetoException {
        // load properties
        Configuration config = new Configuration("twitter.properties");

        String backend = config.getRepository();
        CDataSource dataSource;
        TwitterListener crawler;
        IRepository repository;
        // init URL unshortener
        URLUnshortener unshort = new URLUnshortener(
                config.getConnectionTimeOut(),
                config.getReadTimeOut(),
                config.getCacheSize());
        Cluster cluster = null;

        switch (backend) {
            case "cql":
                cluster = Cluster
                        .builder()
                        .addContactPoint(config.getDatabaseHost())
                        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                        .withLoadBalancingPolicy(
                                new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                        .build();
                Session session = cluster.connect(config.getKeyspace());
                repository = new CassandraRepository(session);
                crawler = new TwitterListener(config, repository);
                break;
            case "sql":
                dataSource = CDataSource.getInstance();
                repository = new MySQLRepository(dataSource, unshort);
                crawler = new TwitterListener(config, repository);
                break;
            default:
                dataSource = CDataSource.getInstance();
                repository = new MySQLRepository(dataSource, unshort);
                crawler = new TwitterListener(config, repository);
                break;
        }

        // init mongo
//        MongoIO mongo = MongoIO.getInstance();
//        mongo.initializeDB("localhost", 27019, "ATCNewSum", "atcnewsum", "2014_scify_atc_@)!$");
        // init repository
//        repository = new MongoRepository(mongo, unshort);
        // init listener
        // start
        crawler.monitor();

        if (cluster != null) {
            cluster.close();
        }
    }

}

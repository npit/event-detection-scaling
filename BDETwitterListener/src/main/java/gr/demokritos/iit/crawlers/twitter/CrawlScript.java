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
import com.mchange.v2.c3p0.AbstractComboPooledDataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import gr.demokritos.iit.crawlers.twitter.factory.Configuration;
import gr.demokritos.iit.crawlers.twitter.repository.CassandraRepository;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository.CrawlEngine;
import gr.demokritos.iit.crawlers.twitter.repository.MySQLRepository;
import gr.demokritos.iit.crawlers.twitter.structures.SearchQuery;
import gr.demokritos.iit.crawlers.twitter.url.URLUnshortener;
import gr.demokritos.iit.crawlers.twitter.utils.QueryLoader;
import java.io.File;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public class CrawlScript {

    public static void main(String[] args) throws IOException, SQLException, PropertyVetoException, IllegalArgumentException, ParseException {
        loadCmdParams(args);
        switch (operation) {
            case MONITOR:
                monitor();
                break;
            case SEARCH:
                search();
                break;
        }
    }

    public static void monitor() throws IOException, SQLException, PropertyVetoException {
        // load properties
        Configuration config = new Configuration(properties);

        String backend = config.getRepository();
        DataSource dataSource;
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
                dataSource = initializeDataSource(config);
                repository = new MySQLRepository(dataSource, unshort);
                crawler = new TwitterListener(config, repository);
                break;
            default:
                dataSource = initializeDataSource(config);
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

    public static void search() throws IOException, SQLException, PropertyVetoException {

        // load properties
        Configuration config = new Configuration(properties);

        String backend = config.getRepository();
        DataSource dataSource;
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
                dataSource = initializeDataSource(config);
                repository = new MySQLRepository(dataSource, unshort);
                crawler = new TwitterListener(config, repository);
                break;
            default:
                dataSource = initializeDataSource(config);
                repository = new MySQLRepository(dataSource, unshort);
                crawler = new TwitterListener(config, repository);
                break;
        }

        // search for each of these queries
        for (SearchQuery query : queries) {
            // start
            crawler.search(query);
        }

        if (cluster != null) {
            cluster.close();
        }

    }

    private static String properties;
    private static Set<SearchQuery> queries;
    private static CrawlEngine operation;

    private static void loadCmdParams(String[] args) throws IllegalArgumentException, ParseException {
        Options options = new Options();
        // add sources option
        options.addOption("q", "queries", true, "provide queries for the crawler");
        // add properties option
        options.addOption("p", "properties", true, "provide properties for the crawler");
        // add properties option
        options.addOption("o",
                "operation",
                true,
                "operation = 'search' | 'monitor':\n "
                + "\tif 'search' is applied, if 'queries' param is not supplied, system will look for ./twitter.queries file to proceed"
                + "\tdefault is 'monitor'");
        // add help
        options.addOption("h", "help", false, "display help message");
        // read params
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        if (args.length == 0 || cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CrawlScript.class.getName(), options);
            System.exit(0);
        }
        boolean load_queries = false;
        if (cmd.hasOption("operation")) {
            operation = CrawlEngine.valueOf(cmd.getOptionValue("operation").toUpperCase());
            if (operation == CrawlEngine.SEARCH) {
                load_queries = true;
            }
        } else {
            operation = CrawlEngine.MONITOR;
        }
        String queries_file;
        // load queries
        if (cmd.hasOption("queries")) {
            queries_file = cmd.getOptionValue("queries");
        } else {
            queries_file = "./twitter.queries";
        }
        // load properties
        if (cmd.hasOption("properties")) {
            properties = cmd.getOptionValue("properties");
        } else {
            properties = "./twitter.properties";
        }
        File fProperties = new File(properties);
        // read values
        try {
            boolean exists = fProperties.exists();
            if (!exists) {
                throw new IllegalArgumentException("please provide a properties file for the crawler");
            }
            if (load_queries) {
                queries = QueryLoader.LoadQueriesFromFile(queries_file, null, null);
            }
        } catch (IOException ex) {
            throw new IllegalArgumentException("please provide a queries file for the crawler", ex);
        }
    }

    private static DataSource initializeDataSource(Configuration config) throws PropertyVetoException {
        AbstractComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:" + config.getDatabaseHost());
        cpds.setUser(config.getDatabaseUserName());
        cpds.setPassword(config.getDatabasePassword());

        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(config.getDataSourceMinPoolSize());
        cpds.setAcquireIncrement(config.getDataSourceAcquireIncrement());
        cpds.setMaxPoolSize(config.getDataSourceMaxPoolSize());
        cpds.setMaxStatements(config.getDataSourceMaxStatements());
        return cpds;
    }
}

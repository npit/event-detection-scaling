/* Copyright 2016 NCSR Demokritos
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package gr.demokritos.iit.crawlers.twitter.factory;

import gr.demokritos.iit.crawlers.twitter.policy.InfluentialCrawlPolicy;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class Configuration {

    private final Properties properties;

    public static final String FILE_SEPARATOR = System.getProperty("file.separator");

    public static final String UTF8 = "UTF-8";

    private Connection dbConnection;

    public Configuration(String configurationFileName) {
        File file = new File(configurationFileName);
        this.properties = new Properties();
        try {
            this.properties.load(new FileInputStream(file));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Configuration() {
        this.properties = new Properties();
    }

    public String getDatabaseHost() {
        return properties.getProperty("databaseHost");
    }

    public String getDatabaseUserName() {
        return properties.getProperty("databaseUsername");
    }

    public String getDatabasePassword() {
        return properties.getProperty("databasePassword");
    }

    public String getRepositoryImpl() {
        return properties.getProperty("repository_impl", "gr.demokritos.iit.crawlers.twitter.repository.CassandraRepository");
    }

    /**
     *
     * @return the keyspace to use (Cassandra backend only)
     */
    public String getKeyspace() {
        return properties.getProperty("keyspace");
    }

    /**
     *
     * @return the database name that we write into
     */
    public String getDatabaseName() {
        return properties.getProperty("databasename");
    }

    public int getConnectionTimeOut() {
        return Integer.parseInt(properties.getProperty("connectTimeout"));
    }

    public int getReadTimeOut() {
        return Integer.parseInt(properties.getProperty("readTimeout"));
    }

    public int getCacheSize() {
        return Integer.parseInt(properties.getProperty("cacheSize"));
    }

    /**
     *
     * @return the directory where all files are stored and read from
     */
    public String getWorkingDir() {
        String sWorkingDir = properties.getProperty("workingDir");

        if (!sWorkingDir.endsWith(FILE_SEPARATOR)) {
            return sWorkingDir + FILE_SEPARATOR;
        } else {
            return sWorkingDir;
        }

    }

    /**
     * Manually sets 'working directory'
     *
     * @param sWorkingDir the path to set
     * @see {@link #getWorkingDir() }
     */
    public void setWorkingDir(String sWorkingDir) {
        properties.put("workingDir", sWorkingDir);
    }

    public String getTwitterConsumerKey() {
        return properties.getProperty("twitterConsumerKey");
    }

    public String getTwitterConsumerKeySecret() {
        return properties.getProperty("twitterConsumerKeySecret");
    }

    public String getTwitterAccessTokken() {
        return properties.getProperty("twitterAccessTokken");
    }

    public String getTwitterAccessTokkenSecret() {
        return properties.getProperty("twitterAccessTokkenSecret");
    }

    public int getDataSourceMinPoolSize() {
        return Integer.parseInt(properties.getProperty("min_pool_size", "5"));
    }

    public int getDataSourceAcquireIncrement() {
        return Integer.parseInt(properties.getProperty("acquire_increment", "5"));
    }

    public int getDataSourceMaxPoolSize() {
        return Integer.parseInt(properties.getProperty("max_pool_size", "20"));
    }

    public int getDataSourceMaxStatements() {
        return Integer.parseInt(properties.getProperty("max_statements", "180"));
    }

    public String getCrawlPolicy() {
        return properties.getProperty("crawl_policy", "gr.demokritos.iit.crawlers.twitter.policy.DefensiveCrawlPolicy");
    }

    public String getCrawlerImpl() {
        return properties.getProperty("crawl_impl", "gr.demokritos.iit.crawlers.twitter.TwitterListener");
    }

    /**
     * in minutes
     *
     * @return
     */
    public int getDelayBetweenCrawls() {
        return Integer.parseInt(properties.getProperty("delay_between_crawls", "1"));
    }

    /**
     * in minutes
     *
     * @return
     */
    public int getCrawlInitialDelay() {
        return Integer.parseInt(properties.getProperty("initial_delay", "1"));
    }

    /**
     *
     * @return the required followers a user must have in order to be crawled,
     * if {@link InfluentialCrawlPolicy} implementation is used, else ignored
     */
    public int getFollowersCutOff() {
        return Integer.parseInt(properties.getProperty("followers_count_cutoff", "100"));
    }
}

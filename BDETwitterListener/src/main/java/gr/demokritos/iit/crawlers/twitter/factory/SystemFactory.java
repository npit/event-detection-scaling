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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.mchange.v2.c3p0.AbstractComboPooledDataSource;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import gr.demokritos.iit.crawlers.twitter.impl.IListener;
import gr.demokritos.iit.crawlers.twitter.exceptions.UndeclaredRepositoryException;
import gr.demokritos.iit.crawlers.twitter.impl.BaseTwitterListener;
import gr.demokritos.iit.crawlers.twitter.policy.ICrawlPolicy;
import gr.demokritos.iit.crawlers.twitter.policy.InfluentialCrawlPolicy;
import gr.demokritos.iit.crawlers.twitter.repository.CassandraRepository;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.repository.MongoRepository;
import gr.demokritos.iit.crawlers.twitter.repository.MySQLRepository;
import gr.demokritos.iit.crawlers.twitter.url.DefaultURLUnshortener;
import gr.demokritos.iit.crawlers.twitter.url.IURLUnshortener;
import java.beans.PropertyVetoException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.EnumMap;
import java.util.Map;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class SystemFactory {

    public static final Logger LOGGER = Logger.getLogger(IListener.class.getName());

    private final Configuration conf;

    private Cluster cluster = null;
    private AbstractComboPooledDataSource cpds = null;

    public SystemFactory(Configuration conf) {
        this.conf = conf;
    }

    public ICrawlPolicy getCrawlPolicy(IRepository repository)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        ICrawlPolicy policy;
        String policy_decl = conf.getCrawlPolicy();
        Class sourceClass = Class.forName(policy_decl);

        // if influential crawl policy
        if (policy_decl.equals(InfluentialCrawlPolicy.class.getName())) {
            // get followers cut-off threshold, and instantiate
            Constructor class_constructor = sourceClass.getConstructor(IRepository.class, long.class);
            policy = (ICrawlPolicy) class_constructor.newInstance(repository, conf.getFollowersCutOff());
        } else {
            policy = (ICrawlPolicy) sourceClass.newInstance();
        }
        return policy;
    }

    public IListener getTwitterListener(Configuration config, IRepository repository)
            throws
            ClassNotFoundException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            IllegalArgumentException,
            InvocationTargetException {
        IListener crawler;
        String crawl_decl = conf.getCrawlerImpl();
        Class sourceClass = Class.forName(crawl_decl);
        Constructor class_constructor = sourceClass.getConstructor(Configuration.class, IRepository.class, ICrawlPolicy.class);
        ICrawlPolicy policy = getCrawlPolicy(repository);
        crawler = (IListener) class_constructor.newInstance(config, repository, policy);
        return crawler;
    }

    public IListener getTwitterListener(Configuration config)
            throws
            ClassNotFoundException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            IllegalArgumentException,
            InvocationTargetException,
            PropertyVetoException {
        IListener crawler;
        String crawl_decl = conf.getCrawlerImpl();
        Class sourceClass = Class.forName(crawl_decl);
        Constructor class_constructor = sourceClass.getConstructor(Configuration.class, IRepository.class, ICrawlPolicy.class);
        IRepository repository = getRepository();
        ICrawlPolicy policy = getCrawlPolicy(repository);
        crawler = (IListener) class_constructor.newInstance(config, repository, policy);
        return crawler;
    }

    /**
     * used for search instantiation
     *
     * @param config
     * @return an instance of {@link BaseTwitterListener}
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws PropertyVetoException
     */
    public IListener getBaseTwitterListener(Configuration config)
            throws
            ClassNotFoundException,
            NoSuchMethodException,
            InstantiationException,
            IllegalAccessException,
            IllegalArgumentException,
            InvocationTargetException,
            PropertyVetoException {
        IListener crawler;
        Class sourceClass = Class.forName(BaseTwitterListener.class.getName());
        Constructor class_constructor = sourceClass.getConstructor(Configuration.class, IRepository.class, ICrawlPolicy.class);
        IRepository repository = getRepository();
        ICrawlPolicy policy = getCrawlPolicy(repository);
        crawler = (IListener) class_constructor.newInstance(config, repository, policy);
        return crawler;
    }

    public IURLUnshortener getDefaultURLUnshortener() {
        IURLUnshortener unshort = new DefaultURLUnshortener(
                conf.getConnectionTimeOut(),
                conf.getReadTimeOut(),
                conf.getCacheSize());
        return unshort;
    }

    public IRepository getRepository() throws PropertyVetoException, UndeclaredRepositoryException {
        IRepository repository;
        String backend = conf.getRepositoryImpl().trim();
        Repository repo_type;
        repo_type = getRepositoryType(backend);
        DataSource dataSource;
        switch (repo_type) {
            case CASSANDRA:
                this.cluster = Cluster
                        .builder()
                        .addContactPoint(conf.getDatabaseHost())
                        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                        .withLoadBalancingPolicy(
                                new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                        .build();
                Session session = cluster.connect(conf.getKeyspace());
                repository = new CassandraRepository(session);
                break;
            case MYSQL:
                dataSource = initializeSQLDataSource();
                repository = new MySQLRepository(dataSource, getDefaultURLUnshortener());
                break;
            default: // default mysql
                dataSource = initializeSQLDataSource();
                repository = new MySQLRepository(dataSource, getDefaultURLUnshortener());
                break;
        }
        return repository;
    }

    private DataSource initializeSQLDataSource() throws PropertyVetoException {
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver");
        cpds.setJdbcUrl("jdbc:" + conf.getDatabaseHost());
        cpds.setUser(conf.getDatabaseUserName());
        cpds.setPassword(conf.getDatabasePassword());

        // the settings below are optional -- c3p0 can work with defaults
        cpds.setMinPoolSize(conf.getDataSourceMinPoolSize());
        cpds.setAcquireIncrement(conf.getDataSourceAcquireIncrement());
        cpds.setMaxPoolSize(conf.getDataSourceMaxPoolSize());
        cpds.setMaxStatements(conf.getDataSourceMaxStatements());
        return cpds;
    }

    public void releaseResources() {
        if (cluster != null) {
            cluster.close();
        }
        if (cpds != null) {
            cpds.close();
        }
    }

    private Repository getRepositoryType(String backend) throws UndeclaredRepositoryException {
        for (Map.Entry<Repository, String> repo : Repository.REPOSITORY_DECLARATIONS.entrySet()) {
            String val = repo.getValue();
            if (backend.equalsIgnoreCase(val)) {
                return repo.getKey();
            }
        }
        throw new UndeclaredRepositoryException();
    }

    public enum Repository {

        MYSQL(MySQLRepository.class.getName()), CASSANDRA(CassandraRepository.class.getName()), MONGO(MongoRepository.class.getName());

        private final String impl_class_decl;

        private Repository(String impl_class_decl) {
            this.impl_class_decl = impl_class_decl;
        }

        public static final EnumMap<Repository, String> REPOSITORY_DECLARATIONS = new EnumMap(Repository.class);

        static {
            REPOSITORY_DECLARATIONS.put(MYSQL, MySQLRepository.class.getName());
            REPOSITORY_DECLARATIONS.put(CASSANDRA, CassandraRepository.class.getName());
            REPOSITORY_DECLARATIONS.put(MONGO, MongoRepository.class.getName());
        }
    }
}

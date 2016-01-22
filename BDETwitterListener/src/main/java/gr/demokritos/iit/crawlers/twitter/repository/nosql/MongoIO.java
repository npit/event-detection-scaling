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
package gr.demokritos.iit.crawlers.twitter.repository.nosql;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class MongoIO {

    /**
     * The Logger Class used for logging various info and higher level messages
     */
    protected final static Logger LOGGER = Logger.getLogger(MongoIO.class.getName());

    private MongoClient storage;
    private DB db = null;
    private String DBName = null;
    private String user;
    private String pw;

    private static MongoIO uniqueInstance;

    private MongoIO() {
    }

    public synchronized static MongoIO getInstance() {
        if (uniqueInstance == null) {
            uniqueInstance = new MongoIO();
        }
        return uniqueInstance;
    }

    /**
     *
     * @param host The host IP
     * @param port The host Port
     * @param DBName The DBName *MUST EXIST
     * @param user the user (must already have write privileges)
     * @param passwd the user password
     *
     * @throws UnknownHostException
     * @throws MongoException
     */
    public void initializeDB(String host, int port, String DBName, String user, String passwd)
            throws UnknownHostException, MongoException {
        this.user = user;
        this.pw = passwd;
        LOGGER.log(Level.INFO, "Initializing connection with database {0}...", DBName);
        // get connection with mongo driver
        this.setMongoInstance(host, port);
        // init DB
        if (this.db == null) {
            try {
                setActiveDatabase(DBName);
            } catch (MongoException ex) {
                LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
                this.DBName = DBName;
            }
        } else {
            if (!this.db.getName().equalsIgnoreCase(DBName)) {
                throw new MongoException("Has been already initialized for " + this.db.getName());
            }
        }
    }

    private void setMongoInstance(String host, int port) throws UnknownHostException {
        this.storage = new MongoClient(host, port);
    }

    private void setActiveDatabase(String DBName) throws MongoException {
        this.db = storage.getDB(DBName);
        if (!this.db.authenticate(this.user, this.pw.toCharArray())) {
            LOGGER.log(Level.INFO, "Could not authenticate with username {0}", user);
        }
    }

    private void createDatabase(String DBName) throws MongoException {
        if (!this.storage.getDatabaseNames().contains(DBName)) {
        } else {
            throw new MongoException(DBName + " already exists. Use initialize existing instead");
        }
    }

    /**
     *
     * @return the DB
     */
    public DB getActiveDatabase() {
        return this.db;
    }

    public void shutdown() {
        this.storage.close();
    }

}

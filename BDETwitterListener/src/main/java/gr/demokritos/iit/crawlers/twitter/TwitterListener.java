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

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.twitter.Extractor;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import gr.demokritos.iit.crawlers.twitter.factory.Configuration;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository.CrawlEngine;
import gr.demokritos.iit.crawlers.twitter.repository.MySQLRepository;
import gr.demokritos.iit.crawlers.twitter.repository.pool.CDataSource;
import gr.demokritos.iit.crawlers.twitter.structures.SearchQuery;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import gr.demokritos.iit.crawlers.twitter.url.URLUnshortener;
import java.util.ArrayList;
import java.util.logging.Level;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterListener implements ICrawler {

    private final String twitterConsumerKey;
    private final String twitterConsumerKeySecret;
    private final String twitterAccessTokken;
    private final String twitterAccessTokkenSecret;

    protected IRepository repository;

    // will extract URLs from the tweet, if any
    protected Extractor extractor;
    // twitter-text lib is a maven snapshot build at 16/05/14
    // cloned from https://github.com/twitter/twitter-text-java.git

    protected Twitter twitter;

    public TwitterListener(String twitterConsumerKey,
            String twitterConsumerKeySecret, String twitterAccessTokken,
            String twitterAccessTokkenSecret, CDataSource dataSource) {
        // init credentials
        this.twitterConsumerKey = twitterConsumerKey;
        this.twitterConsumerKeySecret = twitterConsumerKeySecret;
        this.twitterAccessTokken = twitterAccessTokken;
        this.twitterAccessTokkenSecret = twitterAccessTokkenSecret;
        // init URL extractor
        this.extractor = new Extractor();
        URLUnshortener unshort = new URLUnshortener();
        this.repository = new MySQLRepository(dataSource, unshort);
        // load language Detector
        try {
            DetectorFactory.loadProfile("profiles");
        } catch (LangDetectException e) {
            e.printStackTrace();
        }
    }

    /**
     * Main constructor. Accepts a configuration class that has already read
     * resources
     *
     * @param config the configuration class
     * @param repository
     */
    public TwitterListener(Configuration config, IRepository repository) {
        // init credentials
        this.twitterConsumerKey = config.getTwitterConsumerKey();
        this.twitterConsumerKeySecret = config.getTwitterConsumerKeySecret();
        this.twitterAccessTokken = config.getTwitterAccessTokken();
        this.twitterAccessTokkenSecret = config.getTwitterAccessTokkenSecret();
        // init URL extractor
        this.extractor = new Extractor();

        // init MySQL repository
        this.repository = repository;
//        // load language Detector
//        try {
//            DetectorFactory.loadProfile("profiles");
//        } catch (LangDetectException e) {
//            e.printStackTrace();
//        }        
        //connect
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(twitterConsumerKey)
                .setOAuthConsumerSecret(twitterConsumerKeySecret)
                .setOAuthAccessToken(twitterAccessTokken)
                .setOAuthAccessTokenSecret(twitterAccessTokkenSecret);
        TwitterFactory tf = new TwitterFactory(cb.build());
        // get active instance
        this.twitter = tf.getInstance();
    }

    /**
     * Will load all accounts from the DB and update with the latest tweets,
     * also update existing tweets's retweet counts
     */
    @Override
    public void monitor() {
        System.out.println("Started crawl at " + new Date());
        long engine_id = repository.scheduleInitialized(CrawlEngine.MONITOR);
        /*System.out.println("Remaining API requests before the API limit is reached for the current hour: " + twitter.getRateLimitStatus());
         System.out.println();*/
        // get accounts to monitor from the database
        Collection<SourceAccount> accounts = repository.getAccounts();
        int iCount = 1;
        int iTotal = accounts.size();
        // for each account
        for (SourceAccount sourceAccount : accounts) {
            try {
                String sourceName = sourceAccount.getAccount();
                System.out.format("Parsing '%s': %d/%d accounts%n", sourceName, iCount++, iTotal);
                int iStsCnt = 0;
                // get posts from selected account
                List<Status> statuses = twitter.getUserTimeline(sourceName);
                // process statuses
                processStatuses(statuses, CrawlEngine.MONITOR, engine_id);
                // log done
                System.out.format("Finished: '%s' with %d updates%n", sourceName, iStsCnt);
                System.out.println("----------------------");
            } catch (TwitterException ex) {
                ex.printStackTrace();
            }
        }
        // register finalized schedule
        repository.scheduleFinalized(engine_id, CrawlEngine.MONITOR);
        System.out.println("Finished crawl at " + new Date());
    }

    @Override
    public void search(SearchQuery query) {
        long engine_id = repository.scheduleInitialized(CrawlEngine.SEARCH);
        // query twitter API 
        Query q = new Query(query.getSearchQuery());
        // set lang
        q.setLang(query.getLang());
        // set max possible results
        q.setCount(query.getMaxResultsLimit());
        try {
            System.out.format("searching for '%s'%n", query.getSearchQuery());
            // query
            QueryResult qr = twitter.search(q);
            // get tweets
            List<Status> statuses = qr.getTweets();

            List<Status> filtered = processStatuses(statuses, CrawlEngine.SEARCH, engine_id);
            System.out.format("Finished: '%s' with %d updates%n", query.getSearchQuery(), filtered.size());
        } catch (TwitterException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } finally {
            repository.scheduleFinalized(engine_id, CrawlEngine.SEARCH);
        }
    }

    protected List<Status> processStatuses(List<Status> statuses, CrawlEngine engine_type, long engine_id) {
        List<Status> res = new ArrayList();
        // for each status
        for (Status status : statuses) {
            // if it is a retweet, get the original tweet
            while (status.isRetweet()) {
                status = status.getRetweetedStatus();
            }
            // add status to result list (avoid retweets)
            res.add(status);
            // proceed with storing in twitter repository
            long postID = status.getId();
            User user = status.getUser();
            // check for existance of post in DB
            boolean exists = repository.existsPost(postID);
            // if post already in the db then update post and user info
            if (exists) {
                long retweetCount = status.getRetweetCount();
                repository.updatePost(postID, retweetCount);
                repository.updateUser(user);
            } else {
                // get User ID
                long userID = user.getId();
                // check if user exists in the DB
                boolean exists_user = repository.existsUser(userID);
                String sourceAcc = user.getScreenName();
                if (exists_user) {
                    // if user is in the database, update
                    repository.updateUser(user);
                } else {
                    // else insert
                    repository.insertUser(user);
                }
                // get source ID
                if (!repository.existSource(sourceAcc)) {
                    // also insert as a source
                    repository.saveAccount(sourceAcc, false);
                }
                // get followers of user when post was published
                int followersWhenPublished = user.getFollowersCount();
                // finally, insert the post in the DB
                repository.insertPost(status, userID, user.getScreenName(), followersWhenPublished, engine_type, engine_id);
            }
        }
        return res;
    }

}

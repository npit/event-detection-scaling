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
package gr.demokritos.iit.crawlers.twitter.impl;

import java.util.*;

import gr.demokritos.iit.crawlers.twitter.factory.conf.ITwitterConf;
import gr.demokritos.iit.crawlers.twitter.factory.conf.TConfig;
import static gr.demokritos.iit.crawlers.twitter.factory.TwitterListenerFactory.LOGGER;
import static gr.demokritos.iit.crawlers.twitter.impl.AbstractTwitterRestConsumer.TWITTER_API_CALL_USER_TIMELINE;
import gr.demokritos.iit.crawlers.twitter.policy.ICrawlPolicy;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository.CrawlEngine;
import gr.demokritos.iit.crawlers.twitter.structures.SearchQuery;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import twitter4j.*;

public class BaseTwitterRestConsumer extends AbstractTwitterRestConsumer implements ITwitterRestConsumer {

    public BaseTwitterRestConsumer(ITwitterConf config, IRepository repository) {
        super(config, repository);
    }

    public BaseTwitterRestConsumer(ITwitterConf config, IRepository repository, ICrawlPolicy policy) {
        super(config, repository, policy);
    }

    /**
     * Will load all accounts from the DB and update with the latest tweets,
     * also update existing tweets's retweet counts
     */
    @Override
    public void monitor() {
        LOGGER.info(String.format("Started crawl at %s", new Date().toString()));
        long engine_id = repository.scheduleInitialized(CrawlEngine.MONITOR);
        // get accounts to monitor from the database
        Collection<SourceAccount> accounts = this.getAccounts(config);
        if(accounts == null) throw new IllegalArgumentException();
        //Collection<SourceAccount> accounts = repository.getAccounts();
        LOGGER.info(String.format("Fetched %d accounts from the repository.", (accounts.size())));

        // filter accounts according to policy provided
        policy.filter(accounts);
        LOGGER.info(String.format("%d accounts left post-filtering.", (accounts.size())));
        int iCount = 1;
        int iTotal = accounts.size();
        int iResetCount = 1;
        Map<String, Integer> checkStatus = getRateLimitStatus(TWITTER_API_CALL_USER_TIMELINE);
        long time_started = Calendar.getInstance().getTimeInMillis();
        int remaining_calls_before_limit = checkStatus.get(API_REMAINING_CALLS);
        int seconds_until_reset = checkStatus.get(API_SECONDS_UNTIL_RESET);
        if (remaining_calls_before_limit <= 0) {
            try {
                LOGGER.info(String.format("Reached Rate limit, will sleep for %d seconds to overcome", (seconds_until_reset + 1)));
                Thread.sleep(TimeUnit.MILLISECONDS.convert(seconds_until_reset + 1, TimeUnit.SECONDS));
                checkStatus = getRateLimitStatus(TWITTER_API_CALL_USER_TIMELINE);
                time_started = Calendar.getInstance().getTimeInMillis();
                remaining_calls_before_limit = checkStatus.get(API_REMAINING_CALLS);
                seconds_until_reset = checkStatus.get(API_SECONDS_UNTIL_RESET);
            } catch (InterruptedException ex) {
                LOGGER.severe(ex.getMessage());
            }
        }
        LOGGER.info(String.format("Proceeding to monitor %d accounts.", (accounts.size())));
        // for each account
        for (SourceAccount sourceAccount : accounts) {
            try {
                System.out.println(sourceAccount.toString());
                // check rate limit status
                boolean reset = checkAPICallStatus(iResetCount++, remaining_calls_before_limit, time_started, seconds_until_reset);
                if (reset) {
                    iResetCount = 1;
                    checkStatus = getRateLimitStatus(TWITTER_API_CALL_USER_TIMELINE);
                    time_started = Calendar.getInstance().getTimeInMillis();
                    remaining_calls_before_limit = checkStatus.get(API_REMAINING_CALLS);
                    seconds_until_reset = checkStatus.get(API_SECONDS_UNTIL_RESET);
                }
                // get account name
                String sourceName = sourceAccount.getAccount();
                LOGGER.info(String.format("Parsing '%s': %d/%d accounts", sourceName, iCount++, iTotal));
                // get posts from selected account
                // specify number of posts
                Paging paging = new Paging();
                paging.setCount(sourceAccount.getNumberOfPostsToFetch());
                List<Status> statuses = twitter.getUserTimeline(sourceName, paging);
                // process statuses
                List<Status> res = processStatuses(statuses, CrawlEngine.MONITOR, engine_id);
                // log done
                LOGGER.info(String.format("Finished: '%s' with %d updates", sourceName, res.size()));
            } catch (TwitterException ex) {
                LOGGER.severe(ex.getMessage());
            } catch (InterruptedException ex) {
                LOGGER.severe(ex.getMessage());
            }
        }
        // register finalized schedule
        repository.scheduleFinalized(engine_id, CrawlEngine.MONITOR);
        LOGGER.info(String.format("Finished crawl at %s", new Date().toString()));
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
            LOGGER.info(String.format("searching for '%s'", query.getSearchQuery()));
            // query
            QueryResult qr = twitter.search(q);
            // get tweets
            List<Status> statuses = qr.getTweets();

            List<Status> filtered = processStatuses(statuses, CrawlEngine.SEARCH, engine_id);
            LOGGER.info(String.format("Finished: '%s' with %d updates", query.getSearchQuery(), filtered.size()));
        } catch (TwitterException ex) {
            LOGGER.severe(ex.getMessage());
        } finally {
            repository.scheduleFinalized(engine_id, CrawlEngine.SEARCH);
        }
    }

    @Override
    public void search(Collection<SearchQuery> queries) {
        long engine_id = repository.scheduleInitialized(CrawlEngine.SEARCH);
        try {
            for (SearchQuery query : queries) {
                // query twitter API 
                Query q = new Query(query.getSearchQuery());
                // set lang
                q.setLang(query.getLang());
                // set max possible results
                q.setCount(query.getMaxResultsLimit());
                LOGGER.info(String.format("searching for '%s'", query.getSearchQuery()));
                // query
                QueryResult qr = twitter.search(q);
                // get tweets
                List<Status> statuses = qr.getTweets();

                List<Status> filtered = processStatuses(statuses, CrawlEngine.SEARCH, engine_id);
                LOGGER.info(String.format("Finished: '%s' with %d updates", query.getSearchQuery(), filtered.size()));
            }
        } catch (TwitterException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        } finally {
            repository.scheduleFinalized(engine_id, CrawlEngine.SEARCH);
        }
    }
    @Override
    public void fetch(Collection<Long> tweetIDs)
    {
        long engine_id = repository.scheduleInitialized(CrawlEngine.FETCH);
        List<Status> statuses = new ArrayList<>();
        try {
            for (Long tweetID : tweetIDs) {

                Status status = twitter.showStatus(tweetID);
                if (status == null) { //
                    // don't know if needed - T4J docs are very bad
                } else {
                    statuses.add(status);
                }
            }

            List<Status> filtered = processStatuses(statuses, CrawlEngine.FETCH, engine_id);
            LOGGER.info(String.format("Finished:  %d updates",  filtered.size()));

        }
        catch (TwitterException e)
        {
            System.err.print("Failed to fetch tweets: " + e.getMessage());
            e.printStackTrace();
        }
        finally {
            repository.scheduleFinalized(engine_id, CrawlEngine.FETCH);
        }
        LOGGER.log(Level.INFO,"Done fetching tweets.");
        repository.scheduleFinalized(engine_id,CrawlEngine.FETCH);
    }
}

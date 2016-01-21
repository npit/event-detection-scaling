/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter;

import gr.demokritos.iit.crawlers.twitter.factory.Configuration;
import gr.demokritos.iit.crawlers.twitter.policy.ICrawlPolicy;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.structures.SearchQuery;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import twitter4j.Status;
import twitter4j.TwitterException;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class ScheduledTwitterListener extends AbstractTwitterListener implements ICrawler, Runnable {

    // todo check run-forever imple
    protected ScheduledExecutorService executorService;

    protected long delay;
    protected TimeUnit timeunit;

    public ScheduledTwitterListener(Configuration config, IRepository repository, ICrawlPolicy policy) {
        super(config, repository, policy);
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.delay = 1l;
        this.timeunit = TimeUnit.MINUTES;
    }

    public ScheduledTwitterListener(Configuration config, IRepository repository, long delay, TimeUnit timeunit) {
        super(config, repository);
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.delay = delay;
        this.timeunit = timeunit;
    }

    public ScheduledTwitterListener(Configuration config, IRepository repository, ICrawlPolicy policy, long delay, TimeUnit timeunit) {
        super(config, repository, policy);
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.delay = delay;
        this.timeunit = timeunit;
    }

    @Override
    public void monitor() {
        executorService.scheduleWithFixedDelay(this, 0l, delay, timeunit);
    }

    @Override
    public void search(SearchQuery query) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void search(Collection<SearchQuery> queries) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void run() {
        Collection<SourceAccount> accounts = repository.getAccounts();
        // filter accounts according to policy provided
        policy.select(accounts);
        int iCount = 1;
        int iTotal = accounts.size();
        // for each account
        for (SourceAccount sourceAccount : accounts) {
            try {
//                checkStatus("/statuses/user_timeline");
                String sourceName = sourceAccount.getAccount();
                System.out.format("Parsing '%s': %d/%d accounts%n", sourceName, iCount++, iTotal);
                // get posts from selected account
                List<Status> statuses = twitter.getUserTimeline(sourceName);
                // process statuses
                List<Status> res = processStatuses(statuses, IRepository.CrawlEngine.MONITOR, -1);
                // log done
                System.out.format("Finished: '%s' with %d updates%n", sourceName, res.size());
                System.out.println("----------------------");
            } catch (TwitterException ex) {
                ex.printStackTrace();
            }
        }
    }
}

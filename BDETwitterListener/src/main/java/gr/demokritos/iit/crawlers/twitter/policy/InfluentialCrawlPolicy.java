/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.policy;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class InfluentialCrawlPolicy extends AbstractCrawlPolicy implements ICrawlPolicy {

    private static final String FIELD_FOLLOWERS_COUNT = "followers_count";

    private final long followers_count_cutoff;

    public InfluentialCrawlPolicy(IRepository repository, long followers_count_cutoff) {
        super(repository);
        this.followers_count_cutoff = followers_count_cutoff;
    }

    @Override
    public void filter(Collection<SourceAccount> accounts) {
        Iterator<SourceAccount> it = accounts.iterator();
        while (it.hasNext()) {
            SourceAccount tmp = it.next();
            Map<String, Object> user_info = repository.getUserInfo(tmp.getAccount());
            Long tmpf = (Long) user_info.get(FIELD_FOLLOWERS_COUNT);
            if (tmpf != null) {
                long followers = (long) tmpf;
                // if user currently has less followers than threshold, remove
                if (followers < followers_count_cutoff) {
                    it.remove();
                }
            }
        }
    }
}

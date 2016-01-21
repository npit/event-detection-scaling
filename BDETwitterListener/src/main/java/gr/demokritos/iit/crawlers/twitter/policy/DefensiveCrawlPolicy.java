/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.policy;

import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class DefensiveCrawlPolicy implements ICrawlPolicy {

    /**
     * keep only entries that active = true
     *
     * @param accounts
     */
    @Override
    public void select(Collection<SourceAccount> accounts) {
        Iterator<SourceAccount> it = accounts.iterator();
        while (it.hasNext()) {
            SourceAccount tmp = it.next();
            if (!tmp.getActive()) {
                it.remove();
            }
        }
    }

}

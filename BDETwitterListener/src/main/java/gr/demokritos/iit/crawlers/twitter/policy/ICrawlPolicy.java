/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.policy;

import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import java.util.Collection;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public interface ICrawlPolicy {

    /**
     * provided the full list of accounts, keep those required for crawling
     * according to policy
     *
     * @param accounts
     */
    void select(Collection<SourceAccount> accounts);
}

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
public class AggressiveCrawlPolicy implements ICrawlPolicy {

    @Override
    public void select(Collection<SourceAccount> accounts) {
    }
}

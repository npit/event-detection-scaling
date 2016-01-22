/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.policy;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public abstract class AbstractCrawlPolicy {

    protected IRepository repository;

    public AbstractCrawlPolicy(IRepository repository) {
        this.repository = repository;
    }

    public AbstractCrawlPolicy() {
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.stream;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public abstract class AbstractStatusListener {

    protected final TwitterStream twitterStream;
    protected final IRepository repository;

    public AbstractStatusListener(TwitterStream twitterStream, IRepository repos) {
        this.twitterStream = twitterStream;
        this.repository = repos;
    }

    public AbstractStatusListener(IRepository repos) {

        this.twitterStream = new TwitterStreamFactory().getInstance();
        this.repository = repos;
    }
}

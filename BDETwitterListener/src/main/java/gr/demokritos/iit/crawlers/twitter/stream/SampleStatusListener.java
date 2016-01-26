/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.stream;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class SampleStatusListener extends AbstractStatusListener implements StatusListener, IBDEStream {

    public SampleStatusListener(TwitterStream twitterStream, IRepository repos) {
        super(twitterStream, repos);
    }

    public SampleStatusListener(IRepository repos) {
        super(repos);
    }

    @Override
    public void getStream() {
        this.twitterStream.addListener(this);
        twitterStream.sample("en"); // get english tweets.
    }

    @Override
    public void onStatus(Status status) {
        System.out.println(status.getUser().getName() + " : " + status.getText());
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {

    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning sw) {

    }

    @Override
    public void onException(Exception excptn) {
        excptn.printStackTrace();
    }
}

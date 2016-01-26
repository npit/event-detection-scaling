/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.stream.user;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import java.util.Calendar;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.User;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public abstract class AbstractUserListener {

    protected final TwitterStream twitterStream;
    protected final IRepository repository;

    protected long engine_id;

    public AbstractUserListener(TwitterStream twitterStream, IRepository repos) {
        this.twitterStream = twitterStream;
        this.repository = repos;
        this.engine_id = Calendar.getInstance().getTimeInMillis();
    }

    protected synchronized void processStatus(Status status) {
        // if it is a retweet, get the original tweet
        while (status.isRetweet()) {
            status = status.getRetweetedStatus();
        }
        // proceed with storing in twitter repository
        long postID = status.getId();
        User user = status.getUser();
        // check for existance of post in DB
        boolean exists = repository.existsPost(postID);
        // if post already in the db then update post and user info
        if (exists) {
            repository.updatePost(status);
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
            repository.insertPost(status, userID, user.getScreenName(), followersWhenPublished, IRepository.CrawlEngine.STREAM, engine_id);
        }
    }
}

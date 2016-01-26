/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.stream.user;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.stream.IBDEStream;
import twitter4j.DirectMessage;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.TwitterStream;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserStreamListener;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class UserStatusListener extends AbstractUserListener implements UserStreamListener, IBDEStream {

    public UserStatusListener(TwitterStream twitterStream, IRepository repos) {
        super(twitterStream, repos);
    }

    @Override
    public void getStream() {
        this.twitterStream.addListener(this);
        twitterStream.user();
    }

    @Override
    public void onStatus(Status status) {
        System.out.println("@" + status.getUser().getScreenName() + ": " + status.getText());
        processStatus(status);
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

    @Override
    public void onDeletionNotice(long arg0, long arg1) {

    }

    @Override
    public void onFriendList(long[] arg0) {

    }

    @Override
    public void onFavorite(User arg0, User arg1, Status arg2) {

    }

    @Override
    public void onUnfavorite(User arg0, User arg1, Status arg2) {

    }

    @Override
    public void onFollow(User arg0, User arg1) {

    }

    @Override
    public void onUnfollow(User arg0, User arg1) {

    }

    @Override
    public void onDirectMessage(DirectMessage arg0) {

    }

    @Override
    public void onUserListMemberAddition(User arg0, User arg1, UserList arg2) {

    }

    @Override
    public void onUserListMemberDeletion(User arg0, User arg1, UserList arg2) {

    }

    @Override
    public void onUserListSubscription(User arg0, User arg1, UserList arg2) {

    }

    @Override
    public void onUserListUnsubscription(User arg0, User arg1, UserList arg2) {

    }

    @Override
    public void onUserListCreation(User arg0, UserList arg1) {

    }

    @Override
    public void onUserListUpdate(User arg0, UserList arg1) {

    }

    @Override
    public void onUserListDeletion(User arg0, UserList arg1) {

    }

    @Override
    public void onUserProfileUpdate(User arg0) {

    }

    @Override
    public void onUserSuspension(long arg0) {

    }

    @Override
    public void onUserDeletion(long arg0) {

    }

    @Override
    public void onBlock(User arg0, User arg1) {

    }

    @Override
    public void onUnblock(User arg0, User arg1) {

    }

    @Override
    public void onRetweetedRetweet(User arg0, User arg1, Status arg2) {

    }

    @Override
    public void onFavoritedRetweet(User arg0, User arg1, Status arg2) {

    }

    @Override
    public void onQuotedTweet(User arg0, User arg1, Status arg2) {

    }

}

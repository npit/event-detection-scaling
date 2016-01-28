/* Copyright 2016 NCSR Demokritos
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
public class SampleStatusListener extends AbstractStatusListener implements StatusListener, IStreamConsumer {

    public SampleStatusListener(TwitterStream twitterStream, IRepository repos) {
        super(twitterStream, repos);
    }

    public SampleStatusListener(IRepository repos) {
        super(repos);
    }

    @Override
    public void getStream() {
        this.twitterStream.addListener(this);
        twitterStream.sample("en"); // get tweets in english
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
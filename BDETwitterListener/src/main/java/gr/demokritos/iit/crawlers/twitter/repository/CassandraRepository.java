/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import gr.demokritos.iit.crawlers.twitter.url.URLUnshortener;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import twitter4j.Status;
import twitter4j.User;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public class CassandraRepository extends AbstractRepository implements IRepository {

    public enum Table {

        TWITTER_SOURCE("twitter_source"),
        TWITTER_USER("twitter_user"),
        TWITTER_POST("twitter_post"),
        TWITTER_CREATED_AT_PER_POST("twitter_created_at_per_post"),
        TWITTER_HASHTAG_PER_POST("twitter_hashtag_per_post"),
        TWITTER_LOG("twitter_log"),
        TWITTER_ENGINE_PER_POST("twitter_engine_per_post");
        private final String table_name;

        private Table(String table_name) {
            this.table_name = table_name;
        }

    }

    private final Session session;

    public CassandraRepository(Session session, URLUnshortener unshortenerArg) {
        super(unshortenerArg);
        this.session = session;
    }

    public CassandraRepository(Session session) {
        super();
        this.session = session;
    }

    @Override
    public Collection<SourceAccount> getAccounts() {
        Statement select = QueryBuilder.select().all().from(session.getLoggedKeyspace(), Table.TWITTER_SOURCE.table_name);
        ResultSet results;
        results = session.execute(select);

        Collection<SourceAccount> res = new LinkedHashSet();

        for (Row row : results) {
            String screen_name = row.getString("account_name");
            Boolean active = row.getBool("active");
            if (active) {
                res.add(new SourceAccount(screen_name, active));
                System.out.format("%s %s \n", screen_name, active);
            }
        }
        return res;
    }

    @Override
    public void saveAccount(String accountName) {
        saveAccount(accountName, Boolean.TRUE);
    }

    @Override
    public long insertUser(User user) {
        Statement insert
                = QueryBuilder
                .insertInto(session.getLoggedKeyspace(), Table.TWITTER_USER.table_name)
                .value("user_id", user.getId())
                .value("followers_count", user.getFollowersCount())
                .value("friends_count", user.getFriendsCount())
                .value("listed_count", user.getListedCount())
                .value("location", user.getLocation())
                .value("name", user.getName())
                .value("screen_name", user.getScreenName())
                .value("screen_name", user.getScreenName())
                .value("statuses_count", user.getStatusesCount())
                .value("timezone", user.getTimeZone());
        session.execute(insert);
        return user.getId();
    }

    @Override
    public void updateUser(User user) {
        insertUser(user); // TODO check
    }

    @Override
    public boolean existsUser(long userID) {
        String key = "screen_name";
        Statement select = QueryBuilder.select(key).from(session.getLoggedKeyspace(), Table.TWITTER_USER.table_name).where(eq("user_id", userID));
        ResultSet results = session.execute(select);

        Row one = results.one();
        if (one != null) {
            String sn = one.getString(key);
            if (sn != null && !sn.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void insertPost(Status post, long api_user_id, String account_name, int followersWhenPublished, CrawlEngine engine, long engine_id) {
        Long postID = post.getId();

//       
//       
//       
//            dbConnection = dataSource.getConnection();
//            prepStmt = dbConnection.prepareStatement(
//                    "INSERT INTO twitter_post (post_id, created_at, coordinates, place, "
//                    + "retweet_count, followers_when_published, text, language, url, "
//                    + "twitter_user_id, twitter_source_id) "
//                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS);
//            prepStmt.setLong(1, post.getId());
//            Date createdAt = post.getCreatedAt();
//            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            String dateString = sdf.format(createdAt);
//            prepStmt.setString(2, dateString);
//            GeoLocation geoLocation = post.getGeoLocation();
//            if (geoLocation == null) {
//                prepStmt.setNull(3, Types.VARCHAR);
//            } else {
//                prepStmt.setString(3, post.getGeoLocation().toString());
//            }
//            Place place = post.getPlace();
//            if (place != null) {
//                String sFullName = place.getFullName();
//                String sCountry = place.getCountry();
//                if (sFullName != null) {
//                    prepStmt.setString(4, sFullName);
//                } else if (sCountry != null) {
//                    prepStmt.setString(4, sCountry);
//                }
//            } else {
//                prepStmt.setNull(4, Types.VARCHAR);
//            }
//            prepStmt.setLong(5, post.getRetweetCount());
//            prepStmt.setInt(6, followersWhenPublished);
//            prepStmt.setString(7, sTweet);
//            prepStmt.setString(8, LangDetect.getInstance().identifyLanguage(sTweet));
//            String url = "https://twitter.com/" + sourceName + "/status/" + postID;
//            prepStmt.setString(9, url);
//            prepStmt.setLong(10, userKey);
//            prepStmt.setInt(11, sourceID);
//            prepStmt.execute();
//            generatedKeysSet = prepStmt.getGeneratedKeys();
//            generatedKeysSet.next();
//            // get post ID
//            long generatedKey = generatedKeysSet.getLong(1);
//            // insert hashtags in database
//            for (HashtagEntity hashtagEntities : post.getHashtagEntities()) {
//                insertHashtag(generatedKey, hashtagEntities.getText());
//            }
//            // get URL links, if there 
//            List<String> lsURLs = extractor.extractURLs(sTweet);
//            // unshorten URLs
//            lsURLs = unshortenURLs(lsURLs);
//            // insert them in the DB
//            insertExternalURLs(generatedKey, lsURLs);
    }

    @Override
    public void updatePost(long postID, long retweetCount) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean existsPost(long postID) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void insertHashtag(long generatedKey, String hashtag) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void insertExternalURLs(long generatedID, List<String> lsURLs) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public LinkedHashMap<Integer, String> getTotalRetweets() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long scheduleInitialized(CrawlEngine engine_type) {
        String key = "engine_id";
        Statement select = QueryBuilder
                .select(key)
                .from(session.getLoggedKeyspace(), Table.TWITTER_LOG.table_name)
                .where(eq("engine_type", engine_type.toString())).limit(1);
        ResultSet results = session.execute(select);
        Row one = results.one();

        long max_existing = 0l;

        if (one != null) {
            max_existing = one.getLong(key);
        }
        long current = max_existing + 1;
        Statement insert = QueryBuilder.insertInto(session.getLoggedKeyspace(), Table.TWITTER_LOG.table_name)
                .value("engine_type", engine_type.toString())
                .value("engine_id", current)
                .value("started", new Date().getTime());
        session.execute(insert);

        return current;
    }

    @Override
    public void scheduleFinalized(long schedule_id, CrawlEngine engine_type) {
        Statement update = QueryBuilder.update(session.getLoggedKeyspace(), Table.TWITTER_LOG.table_name)
                .with(QueryBuilder.set("ended", new Date().getTime()))
                .where(QueryBuilder.eq("engine_type", engine_type.toString())).and(QueryBuilder.eq("engine_id", schedule_id));
        session.execute(update);
    }

    @Override
    public boolean existSource(String sourceAcc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void saveAccount(String accountName, boolean active) {
        Statement insert
                = QueryBuilder
                .insertInto(Table.TWITTER_SOURCE.table_name)
                .value("account_name", accountName)
                .value("active", active);
        session.execute(insert);
    }
}

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
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.UDTMapper;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import gr.demokritos.iit.crawlers.twitter.structures.TGeoLoc;
import gr.demokritos.iit.crawlers.twitter.structures.TPlace;
import gr.demokritos.iit.crawlers.twitter.url.IURLUnshortener;
import gr.demokritos.iit.crawlers.twitter.utils.langdetect.CybozuLangDetect;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
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
        TWITTER_POSTS_PER_DATE("twitter_posts_per_date"),
        TWITTER_POSTS_PER_HASHTAG("twitter_posts_per_hashtag"),
        TWITTER_POSTS_PER_EXTERNAL_URL("twitter_posts_per_external_url"),
        TWITTER_POSTS_PER_COORDINATES("twitter_posts_per_coordinates"),
        TWITTER_POSTS_PER_PLACE("twitter_posts_per_place"),
        TWITTER_POSTS_PER_ENGINE("twitter_posts_per_engine"),
        TWITTER_LOG("twitter_log");
        private final String table_name;

        private Table(String table_name) {
            this.table_name = table_name;
        }

    }

    private final Session session;

    private final UDTMapper<TGeoLoc> geolocation_mapper;
    private final UDTMapper<TPlace> place_mapper;

    public CassandraRepository(Session session, IURLUnshortener unshortenerArg) {
        super(unshortenerArg);
        this.session = session;
        this.geolocation_mapper = new MappingManager(session).udtMapper(TGeoLoc.class);
        this.place_mapper = new MappingManager(session).udtMapper(TPlace.class);
    }

    /**
     * default constructor
     *
     * @param session
     */
    public CassandraRepository(Session session) {
        super();
        this.session = session;
        this.geolocation_mapper = new MappingManager(session).udtMapper(TGeoLoc.class);
        this.place_mapper = new MappingManager(session).udtMapper(TPlace.class);
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
            res.add(new SourceAccount(screen_name, active));
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
                .value("account_name", user.getScreenName())
                .value("statuses_count", user.getStatusesCount())
                .value("timezone", user.getTimeZone());
        session.execute(insert);
        return user.getId();
    }

    @Override
    public void updateUser(User user) {
        insertUser(user);
    }

    @Override
    public boolean existsUser(long userID) {
        String key = "account_name";
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
        if (post == null) {
            return;
        }
        String tweet = post.getText();
        if (tweet == null || tweet.trim().isEmpty()) {
            return;
        }
        tweet = tweet.trim();

        // identify tweet language
        String tweet_identified_lang = CybozuLangDetect.getInstance().identifyLanguage(tweet);

        Long post_id = post.getId();

        String coordinates = "";
        GeoLocation geoLocation = post.getGeoLocation();
        // create the UDT for geolocation 
        TGeoLoc geoloc;
        if (geoLocation != null) {
            coordinates = post.getGeoLocation().toString();
            geoloc = new TGeoLoc(post.getGeoLocation().getLatitude(), post.getGeoLocation().getLongitude());
        } else {
            geoloc = new TGeoLoc(0.0, 0.0);
        }
        // generate the geolocation bucket to utilize later
        String geo_bucket_literal = extractGeolocationLiteral(geoLocation);
        Place place = post.getPlace();
        TPlace tplace;
        String sPlace = "";
        if (place != null) {
            String sFullName = place.getFullName();
            String sCountry = place.getCountry();
            if (sFullName != null) {
                sPlace = sFullName;
            } else if (sCountry != null) {
                sPlace = sCountry;
            }
            tplace = new TPlace(place);
        } else {
            tplace = new TPlace();
        }

        long timestamp_created = post.getCreatedAt().getTime();

        // extract tweet permalink
        String permalink = "https://twitter.com/" + account_name + "/status/" + post_id;

        // get URL links, if there 
        List<String> external_links = extractor.extractURLs(tweet);
        // unshorten URLs
        external_links = unshortenURLs(external_links);

        // insert post in twitter_post
        Statement insert
                = QueryBuilder
                .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POST.table_name)
                .value("post_id", post_id)
                .value("language", tweet_identified_lang)
                .value("account_name", account_name)
                .value("coordinates", coordinates)
                .value("created_at", timestamp_created)
                .value("external_links", external_links)
                .value("followers_when_published", followersWhenPublished)
                .value("place", sPlace)
                .value("retweet_count", post.getRetweetCount())
                .value("tweet", tweet)
                .value("url", permalink);
        session.execute(insert);

        // insert metadata in twitter_hashtags_per_post
        // extract hashtags
        HashtagEntity[] hashtagEntities = post.getHashtagEntities();

        for (HashtagEntity hashtag : hashtagEntities) {
            Statement insert_hashtag
                    = QueryBuilder
                    .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POSTS_PER_HASHTAG.table_name)
                    .value("hashtag", hashtag.getText())
                    .value("created_at", timestamp_created)
                    .value("post_id", post_id)
                    .value("account_name", account_name)
                    .value("language", tweet_identified_lang)
                    .value("tweet", tweet)
                    .value("url", permalink);
            session.execute(insert_hashtag);
        }

        // insert metadata in twitter_external_urls_per_post
        for (String external_url : external_links) {
            Statement insert_external_url
                    = QueryBuilder
                    .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POSTS_PER_EXTERNAL_URL.table_name)
                    .value("external_url", external_url)
                    .value("created_at", timestamp_created)
                    .value("post_id", post_id)
                    .value("account_name", account_name)
                    .value("language", tweet_identified_lang)
                    .value("tweet", tweet)
                    .value("url", permalink);
            session.execute(insert_external_url);
        }
        // insert metadata in twitter_created_at_per_post
        // extract year_month data to divide to buckets.
        String year_month_bucket = extractYearMonthLiteral(post.getCreatedAt());

        Statement insert_created_at
                = QueryBuilder
                .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POSTS_PER_DATE.table_name)
                .value("year_month_bucket", year_month_bucket)
                .value("created_at", timestamp_created)
                .value("post_id", post_id)
                .value("account_name", account_name)
                .value("language", tweet_identified_lang)
                .value("tweet", tweet)
                .value("url", permalink);
        session.execute(insert_created_at);

        if (!geo_bucket_literal.isEmpty()) {
            System.out.println("inserting: " + geoloc.toString());
            // insert metadata at twitter_coordinates_per_post
            Statement insert_coords
                    = QueryBuilder
                    .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POSTS_PER_COORDINATES.table_name)
                    .value("geo_bucket", geo_bucket_literal)
                    .value("created_at", timestamp_created)
                    .value("geolocation", geolocation_mapper.toUDT(geoloc))
                    .value("post_id", post_id)
                    .value("account_name", account_name)
                    .value("language", tweet_identified_lang)
                    .value("tweet", tweet)
                    .value("url", permalink);
            session.execute(insert_coords);
        }

        if (!sPlace.isEmpty()) {
            System.out.println("inserting: " + tplace.toString());
            // insert metadata at twitter_place_per_post
            Statement insert_place
                    = QueryBuilder
                    .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POSTS_PER_PLACE.table_name)
                    .value("place_literal", sPlace)
                    .value("created_at", timestamp_created)
                    .value("post_id", post_id)
                    .value("account_name", account_name)
                    .value("language", tweet_identified_lang)
                    .value("place", place_mapper.toUDT(tplace))
                    .value("tweet", tweet)
                    .value("url", permalink);
            session.execute(insert_place);
        }
        // insert metadata at twitter_engine_per_post
        Statement insert_engine
                = QueryBuilder
                .insertInto(session.getLoggedKeyspace(), Table.TWITTER_POSTS_PER_ENGINE.table_name)
                .value("engine_type", engine.toString().toLowerCase())
                .value("engine_id", engine_id)
                .value("post_id", post_id);
        session.execute(insert_engine);
    }

    @Override
    public void updatePost(Status post) {
        String lang = CybozuLangDetect.getInstance().identifyLanguage(post.getText().trim());
        Statement update = QueryBuilder.update(session.getLoggedKeyspace(), Table.TWITTER_POST.table_name)
                .with(QueryBuilder.set("retweet_count", post.getRetweetCount()))
                .where(QueryBuilder.eq("post_id", post.getId())).and(QueryBuilder.eq("language", lang));
        String post_lang = post.getLang();
        session.execute(update);
        if (post_lang != null && !post_lang.isEmpty()) {
            if (!lang.equalsIgnoreCase(post_lang)) {
                LOGGER.warning(String.format("Mismatched language for Status[%d]: from post: '%s', from lang_detect: '%s'", post.getId(), post_lang, lang));
            }
        }
    }

    @Override
    public boolean existsPost(long post_id) {
        Statement select
                = QueryBuilder
                .select()
                .column("language")
                .from(session.getLoggedKeyspace(), Table.TWITTER_POST.table_name)
                .where(eq("post_id", post_id)).limit(1);
        ResultSet result = session.execute(select);

        if (result != null) {
            Row one = result.one();
            if (one != null) {
                String lang = one.getString("language");
                return (lang != null && !lang.isEmpty());
            }
        }
        return false;
    }

    @Override
    public LinkedHashMap<Integer, String> getTotalRetweets() {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long scheduleInitialized(CrawlEngine engine_type) {
        String key = "engine_id";
        Statement select = QueryBuilder
                .select(key)
                .from(session.getLoggedKeyspace(), Table.TWITTER_LOG.table_name)
                .where(eq("engine_type", engine_type.toString().toLowerCase())).limit(1);
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
                .where(QueryBuilder.eq("engine_type", engine_type.toString().toLowerCase())).and(QueryBuilder.eq("engine_id", schedule_id));
        session.execute(update);
    }

    @Override
    public boolean existSource(String account_name) {
        Statement select
                = QueryBuilder
                .select()
                .countAll()
                .from(session.getLoggedKeyspace(), Table.TWITTER_SOURCE.table_name)
                .where(eq("account_name", account_name)).limit(1);
        ResultSet result = session.execute(select);

        if (result != null) {
            Row one = result.one();
            if (one != null) {
                long cnt = one.getLong(0);
                return cnt > 0;
            }
        }
        return false;
    }

    @Override
    public void saveAccount(String account_name, boolean active) {
        Statement insert
                = QueryBuilder
                .insertInto(Table.TWITTER_SOURCE.table_name)
                .value("account_name", account_name)
                .value("active", active);
        session.execute(insert);
    }
}

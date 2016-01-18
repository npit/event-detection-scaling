/*
 * Copyright 2015 SciFY NPO <info@scify.org>.
 *
 * This product is part of the NewSum Free Software.
 * For more information about NewSum visit
 *
 * 	http://www.scify.gr/site/en/projects/completed/newsum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * If this code or its output is used, extended, re-engineered, integrated,
 * or embedded to any extent in another software or hardware, there MUST be
 * an explicit attribution to this work in the resulting source code,
 * the packaging (where such packaging exists), or user interface
 * (where such an interface exists).
 *
 * The attribution must be of the form "Powered by NewSum, SciFY"
 *
 */
package gr.demokritos.iit.crawlers.twitter.url;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
/**
 * Expand short urls. Works with all the major url shorteners (t.co, bit.ly,
 * fb.me, is.gd, goo.gl, etc) 
 * // from http://www.baeldung.com/unshorten-url-httpclient
 *
 * @author Baeldung, George K.
 * 
*/
public class URLUnshortener {

    private static final Logger LOGGER = Logger.getLogger(URLUnshortener.class.getName());

    public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    public static final int DEFAULT_READ_TIMEOUT = 2000;
    public static final int DEFAULT_CACHE_SIZE = 10000;

    private static final String AGENT = "ATC_NewSumBot/0.2 (http://www.atc.gr | http://www.scify.org)";

    private final LRUCache<String, String> cache;

    private DefaultHttpClient client;

    public URLUnshortener() {
        this(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_CACHE_SIZE);
    }

    /**
     * @param connectTimeout HTTP connection timeout, in ms
     * @param readTimeout HTTP read timeout, in ms
     * @param cacheSize Number of resolved URLs to maintain in cache
     */
    public URLUnshortener(int connectTimeout, int readTimeout, int cacheSize) {
        // build cache
        this.cache = LRUCache.build(cacheSize);
        // init http client
        HttpParams httpParameters = new BasicHttpParams();
        HttpProtocolParams.setUserAgent(httpParameters, AGENT);        
        // do not allow redirects
        httpParameters.setParameter("http.protocol.handle-redirects", false);
        // apply timeout params
        httpParameters.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, readTimeout);
        httpParameters.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connectTimeout);
        // init client
        this.client = new DefaultHttpClient(httpParameters);
    }

    /**
     * 
     * @param url the short URL
     * @return the actual long URL
     */
    protected String expandSingleLevel(String url) {
        // if in cache, return
        String existing = cache.get(url);
        if (existing != null) {
            return existing;
        }
        // init http entities
        HttpGet request = null;
        HttpEntity httpEntity = null;
        InputStream entityContentStream = null;
        try {
            request = new HttpGet(url);
            // get the response
            HttpResponse httpResponse = client.execute(request);
            // get entity
            httpEntity = httpResponse.getEntity();
            // get content
            entityContentStream = httpEntity.getContent();
            // get status code
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            // if not a redirection, this is it
            if (statusCode != HttpURLConnection.HTTP_MOVED_PERM
                    && statusCode != HttpURLConnection.HTTP_MOVED_TEMP) {
                return url;
            }
            // obtain headers
            Header[] headers = httpResponse.getHeaders(HttpHeaders.LOCATION);
            Preconditions.checkState(headers.length == 1);
            // get redirected URL
            String newUrl = headers[0].getValue();
            // update cache
            cache.put(url, newUrl);
            // return 
            return newUrl;
        } catch (IllegalArgumentException uriEx) {
            LOGGER.log(Level.SEVERE, uriEx.getMessage(), uriEx);
            return url;
        } catch (IllegalStateException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return url;
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return url;
        } finally {
            if (request != null) {
                request.releaseConnection();
            }
            if (entityContentStream != null) {
                try {
                    entityContentStream.close();
                } catch (IOException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }
            }
            if (httpEntity != null) {
                try {
                    EntityUtils.consume(httpEntity);
                } catch (IOException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /**
     * Will invoke {@link #expandSingleLevel(java.lang.String)} 
     * as many times needed to reach to the final URL
     * @param urlArg the shortened URL
     * @return the full URL
     */
    public String expand(String urlArg) {
        String originalUrl = urlArg;
        // get new URL
        String newUrl = expandSingleLevel(originalUrl);
        // if new URL equals the input URL, this is it
        while (!originalUrl.equals(newUrl)) {
            originalUrl = newUrl;
            newUrl = expandSingleLevel(originalUrl);
        }
        return newUrl;
    }

//    public static void main(String[] args) {
//
//        URLUnshortener urlus = new URLUnshortener();
////        String s = "http://t.co/HdrcwFI59K";
//        String s = "http://t.co/1XnGVFj8Tc";
//        urlus.expand(s);
//
//    }
}

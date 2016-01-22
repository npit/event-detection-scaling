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
package gr.demokritos.iit.crawlers.twitter.url;

import com.google.common.base.Preconditions;
import static gr.demokritos.iit.crawlers.twitter.factory.SystemFactory.LOGGER;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
/**
 * Expand short urls. Works with all the major url shorteners (t.co, bit.ly,
 * fb.me, is.gd, goo.gl, etc) // from
 * http://www.baeldung.com/unshorten-url-httpclient
 *
 * @author Baeldung, George K.
 * 
*/
public class DefaultURLUnshortener extends AbstractURLUnshortener implements IURLUnshortener {

    public DefaultURLUnshortener() {
        super();
    }

    public DefaultURLUnshortener(int connectTimeout, int readTimeout, int cacheSize) {
        super(connectTimeout, readTimeout, cacheSize);
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
        } catch (IllegalArgumentException | IllegalStateException | IOException uriEx) {
            LOGGER.log(Level.WARNING, String.format("%s: %s", uriEx.getMessage(), url), uriEx);
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
     * Will invoke {@link #expandSingleLevel(java.lang.String)} as many times
     * needed to reach to the final URL
     *
     * @param urlArg the shortened URL
     * @return the full URL
     */
    @Override
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
}

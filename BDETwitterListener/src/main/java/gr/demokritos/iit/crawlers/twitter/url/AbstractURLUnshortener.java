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

import gr.demokritos.iit.crawlers.twitter.cache.LRUCache;
import static gr.demokritos.iit.crawlers.twitter.url.IURLUnshortener.DEFAULT_CACHE_SIZE;
import static gr.demokritos.iit.crawlers.twitter.url.IURLUnshortener.DEFAULT_CONNECT_TIMEOUT;
import static gr.demokritos.iit.crawlers.twitter.url.IURLUnshortener.DEFAULT_READ_TIMEOUT;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public abstract class AbstractURLUnshortener {

    protected static final String AGENT = "BigDataEuropeCrawler/0.1 (http://www.big-data-europe.eu/)";

    protected final LRUCache<String, String> cache;

    protected HttpClient client;

    public AbstractURLUnshortener() {
        this(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_CACHE_SIZE);
    }

    /**
     * @param connectTimeout HTTP connection timeout, in ms
     * @param readTimeout HTTP read timeout, in ms
     * @param cacheSize Number of resolved URLs to maintain in cache
     */
    public AbstractURLUnshortener(int connectTimeout, int readTimeout, int cacheSize) {
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
}

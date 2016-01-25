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
import java.net.HttpURLConnection;
import java.util.Objects;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public abstract class AbstractURLUnshortener {

    protected static final String AGENT = "BigDataEuropeCrawler/0.1 (http://www.big-data-europe.eu/)";

    protected final LRUCache<String, URLData> cache;

    protected HttpClient client;

    public AbstractURLUnshortener() {
        this(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_CACHE_SIZE);
    }

    /**
     * @param connection_timeout HTTP connection timeout, in ms
     * @param socket_timeout HTTP read timeout, in ms
     * @param url_cache_size Number of resolved URLs to maintain in cache
     */
    public AbstractURLUnshortener(int connection_timeout, int socket_timeout, int url_cache_size) {
        // build cache
        this.cache = LRUCache.build(url_cache_size);
        // init http client
//        HttpParams httpParameters = new BasicHttpParams();
//        HttpProtocolParams.setUserAgent(httpParameters, AGENT);
//        // do not allow redirects
//        httpParameters.setParameter("http.protocol.handle-redirects", false);
//        // apply timeout params
//        httpParameters.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, socket_timeout);
//        httpParameters.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connection_timeout);
        // init client
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(connection_timeout)
                .setConnectionRequestTimeout(2000)
                .setSocketTimeout(socket_timeout)
                .setRedirectsEnabled(false)
                .build();

        this.client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    protected boolean isRedirect(int status_code) {
        return status_code == HttpURLConnection.HTTP_MOVED_PERM || status_code == HttpURLConnection.HTTP_MOVED_TEMP;
    }

    protected class URLData {

        private final int http_code;
        private final String url;

        public URLData(int http_code, String url) {
            this.http_code = http_code;
            this.url = url;
        }

        public int getHttpCode() {
            return http_code;
        }

        public String getUrl() {
            return url;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 89 * hash + Objects.hashCode(this.url);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final URLData other = (URLData) obj;
            if (!Objects.equals(this.url, other.url)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "URLData{" + "http_code=" + http_code + ", url=" + url + '}';
        }
    }
}

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
package gr.demokritos.iit.crawlers.twitter.structures;

import java.util.Objects;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class SearchQuery {

    private final String search_query;
    private final String lang;
    private final int max_results;

    private static final int MAX_RESULTS_ALLOWED = 100;
    private static final int RECOMMENDED_RESULTS_ALLOWED = 80;

    public SearchQuery(String search_query, String lang, int max_results) {
        this.search_query = search_query;
        this.lang = lang;
        this.max_results = Math.min(max_results, MAX_RESULTS_ALLOWED);
    }

    public SearchQuery(String search_query, String lang) {
        this.search_query = search_query;
        this.lang = lang;
        this.max_results = RECOMMENDED_RESULTS_ALLOWED;
    }

    public String getSearchQuery() {
        return search_query;
    }

    public String getLang() {
        return lang;
    }

    public int getMaxResultsLimit() {
        return max_results;
    }

    @Override
    public String toString() {
        return "SearchQuery{" + "search_query=" + search_query + ", lang=" + lang + ", max_results=" + max_results + '}';
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(this.search_query);
        hash = 59 * hash + Objects.hashCode(this.lang);
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
        final SearchQuery other = (SearchQuery) obj;
        if (!Objects.equals(this.search_query, other.search_query)) {
            return false;
        }
        if (!Objects.equals(this.lang, other.lang)) {
            return false;
        }
        return true;
    }
}

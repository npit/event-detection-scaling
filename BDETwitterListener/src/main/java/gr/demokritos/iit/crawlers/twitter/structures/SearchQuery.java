/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.structures;

import java.util.Objects;

/**
 *
 * @author George K. <gkiom@scify.org>
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

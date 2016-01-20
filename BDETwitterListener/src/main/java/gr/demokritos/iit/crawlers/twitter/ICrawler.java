/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter;

import gr.demokritos.iit.crawlers.twitter.structures.SearchQuery;
import java.util.Collection;
import java.util.logging.Logger;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public interface ICrawler {

    Logger LOGGER = Logger.getLogger(ICrawler.class.getName());

    /**
     * monitor the user accounts provided in the DB
     */
    public void monitor();

    /**
     * search using twitter API
     *
     * @param query
     */
    public void search(SearchQuery query);

    /**
     * search iteratively using twitter API
     *
     * @param queries
     */
    public void search(Collection<SearchQuery> queries);

}

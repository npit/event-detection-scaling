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

    public void monitor();

    public void search(SearchQuery query);

    public void search(Collection<SearchQuery> queries);

}

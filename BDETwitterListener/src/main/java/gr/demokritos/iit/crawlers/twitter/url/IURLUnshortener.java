/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.url;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public interface IURLUnshortener {

    int DEFAULT_CACHE_SIZE = 10000;
    int DEFAULT_CONNECT_TIMEOUT = 2000;
    int DEFAULT_READ_TIMEOUT = 2000;

    /**
     * Will try to expand the shortened URL as many times as needed to reach the
     * final endpoint. Will return the provided shortened URL on fail
     *
     * @param urlArg the shortened URL
     * @return the full URL
     */
    String expand(String urlArg);

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.policies;

import gr.demokritos.iit.geonames.client.DefaultGeonamesClient;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class Policies {

    public enum CrawlImpl {

    }

    public enum StorageImpl {

    }

    public enum CrawlPolicyImpl {

    }

    public enum GeoNamesPolicyImpl {

        NONE(""), DEFAULT(DefaultGeonamesClient.class.getName());
        private String impl;

        private GeoNamesPolicyImpl(String impl) {
            this.impl = impl;
        }

    }
}

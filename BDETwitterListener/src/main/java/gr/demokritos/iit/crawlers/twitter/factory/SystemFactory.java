/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.factory;

import gr.demokritos.iit.crawlers.twitter.ICrawler;
import gr.demokritos.iit.crawlers.twitter.policy.ICrawlPolicy;
import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import java.lang.reflect.Constructor;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class SystemFactory {

    private Configuration conf;

    public SystemFactory(Configuration conf) {
        this.conf = conf;
    }

    public ICrawlPolicy getCrawlPolicy() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        ICrawlPolicy policy;
        String policy_decl = conf.getCrawlPolicy();
        // get class
        Class sourceClass = Class.forName(policy_decl);
//        // get class string constructor
//        Constructor class_constructor = sourceClass.getConstructor(String.class);
//        // get new instance using 1 argument (string) constructor.
//        // the string param must be provided
//        policy = (ICrawlPolicy) class_constructor.newInstance(string_param);
        policy = (ICrawlPolicy) sourceClass.newInstance();
        return policy;
    }

    public ICrawler getTwitterListener(Configuration config, IRepository repository, ICrawlPolicy policy) throws ClassNotFoundException, NoSuchMethodException {
        ICrawler crawler;

        String crawl_decl = conf.getCrawlerImpl();
        Class sourceClass = Class.forName(crawl_decl);

        Constructor class_constructor = sourceClass.getConstructor(Configuration.class, IRepository.class, ICrawlPolicy.class);
        
        return crawler;
    }

}

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
package gr.demokritos.iit.crawlers.twitter.policy;

import gr.demokritos.iit.crawlers.twitter.repository.IRepository;
import gr.demokritos.iit.crawlers.twitter.structures.SourceAccount;
import java.util.Collection;
import java.util.Iterator;

/**
 * filter accounts that are declared as active
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class DefensiveCrawlPolicy extends AbstractCrawlPolicy implements ICrawlPolicy {

    public DefensiveCrawlPolicy(IRepository repository) {
        super(repository);
    }

    /**
     * keep only entries that active = true
     *
     * @param accounts
     */
    @Override
    public void filter(Collection<SourceAccount> accounts) {
        Iterator<SourceAccount> it = accounts.iterator();
        while (it.hasNext()) {
            SourceAccount tmp = it.next();
            if (!tmp.getActive()) {
                it.remove();
            }
        }
    }
}

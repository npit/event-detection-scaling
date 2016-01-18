/*
 * Copyright 2015 SciFY NPO <info@scify.org>.
 *
 * This product is part of the NewSum Free Software.
 * For more information about NewSum visit
 *
 * 	http://www.scify.gr/site/en/projects/completed/newsum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * If this code or its output is used, extended, re-engineered, integrated,
 * or embedded to any extent in another software or hardware, there MUST be
 * an explicit attribution to this work in the resulting source code,
 * the packaging (where such packaging exists), or user interface
 * (where such an interface exists).
 *
 * The attribution must be of the form "Powered by NewSum, SciFY"
 *
 */
package gr.demokritos.iit.crawlers.twitter.repository;

import com.twitter.Extractor;
import java.util.ArrayList;
import java.util.List;
import gr.demokritos.iit.crawlers.twitter.url.URLUnshortener;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public class AbstractRepository {

    protected Extractor extractor;
    protected URLUnshortener unshortener;

    /**
     *
     * @param unshortenerArg the unshortener
     */
    public AbstractRepository(URLUnshortener unshortenerArg) {
        // init twitter extractor
        this.extractor = new Extractor();
        // init URLUnshortener
        this.unshortener = unshortenerArg;
    }

    /**
     * Uses default params for URL unshortener
     *
     */
    public AbstractRepository() {
        // init twitter extractor
        this.extractor = new Extractor();
        // init URLUnshortener
        this.unshortener = new URLUnshortener();
    }

    /**
     * Calls {@link URLUnshortener} to expand shortened URLs
     *
     * @param lsURLs the list of URLs contained in a tweet
     * @return a list of unshortened URLs
     */
    protected List<String> unshortenURLs(List<String> lsURLs) {
        List<String> lsRes = new ArrayList();
        for (String tCo : lsURLs) {
            lsRes.add(unshortener.expand(tCo));
        }
        return lsRes;
    }
}

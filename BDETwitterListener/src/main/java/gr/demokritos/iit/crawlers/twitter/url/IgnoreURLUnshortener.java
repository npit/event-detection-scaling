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

/**
 * does not try to expand URL, instead returns the string provided
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class IgnoreURLUnshortener extends AbstractURLUnshortener implements IURLUnshortener {

    public IgnoreURLUnshortener() {
        super();
    }

    public IgnoreURLUnshortener(int connectTimeout, int readTimeout, int cacheSize) {
        super(connectTimeout, readTimeout, cacheSize);
    }

    @Override
    public String expand(String urlArg) {
        return urlArg;
    }
}

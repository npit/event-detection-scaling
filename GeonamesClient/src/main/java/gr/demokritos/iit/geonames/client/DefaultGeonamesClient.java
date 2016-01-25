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
package gr.demokritos.iit.geonames.client;

import java.util.List;
import org.geonames.Toponym;
import org.geonames.ToponymSearchCriteria;
import org.geonames.ToponymSearchResult;
import org.geonames.WebService;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class DefaultGeonamesClient implements IGeonamesClient {

    private String client_name;

    public DefaultGeonamesClient(String client_name) {
        this.client_name = client_name;
    }

    @Override
    public ToponymSearchResult getCoordinates(String place_literal) throws Exception {
        WebService.setUserName(client_name);
        ToponymSearchCriteria searchCriteria = new ToponymSearchCriteria();
        searchCriteria.setQ(place_literal);
        return WebService.search(searchCriteria);
    }

    @Override
    public List<Toponym> findNearby(double lat, double lng) throws Exception {
        WebService.setUserName(client_name);
        return WebService.findNearbyPlaceName(lat, lng);
    }

}

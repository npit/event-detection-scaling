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
package gr.demokritos.iit.location.mapping.client;

import java.util.Map;
import javax.ws.rs.core.Response;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public interface IRestClient {

    /**
     *
     * @param base_url
     * @param params
     * @param class_of_entity the entity class, if other than string
     * @return the entity received from the couch API
     * @throws Exception
     */
    String execGet(String base_url, Map<String, String> params, Class class_of_entity) throws Exception;

    String execJSONPost(String base_url, String json_data, Class class_of_entity) throws Exception;
}

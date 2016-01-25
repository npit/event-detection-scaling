/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.geonames.client;

import java.util.List;
import org.geonames.Toponym;
import org.geonames.ToponymSearchResult;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public interface IGeonamesClient {

    ToponymSearchResult getCoordinates(String place_literal) throws Exception;

    List<Toponym> findNearby(double lat, double lng) throws Exception;

    // rate limit exceptions 
    // error_code, message
//      18	daily limit of credits exceeded
//      19	hourly limit of credits exceeded
//      20	weekly limit of credits exceeded
}

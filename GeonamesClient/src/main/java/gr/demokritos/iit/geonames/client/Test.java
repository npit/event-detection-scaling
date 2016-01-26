/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.geonames.client;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.geonames.Toponym;
import org.geonames.ToponymSearchResult;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class Test {

    public static void main(String[] args) throws Exception {

        String client_name = "bdedevcrawl";
        IGeonamesClient cl = new DefaultGeonamesClient(client_name);

        List<Toponym> findNearby = cl.findNearby(40.11612, -122.23352);
        
        for (Toponym tnim : findNearby) {
            System.out.println(tnim.toString());
        }
        
        findNearby = cl.findNearby(40.63159, 22.94748);
        
        for (Toponym tnim : findNearby) {
            System.out.println(tnim.toString());
        }
        
        ToponymSearchResult coordinates = cl.getCoordinates("Chania, Hellas");
    
        for (Toponym ts : coordinates.getToponyms()) {
            System.out.println(ts.toString());
        }
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.utils;

import static gr.demokritos.iit.crawlers.twitter.ICrawler.LOGGER;
import gr.demokritos.iit.crawlers.twitter.structures.SearchQuery;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class QueryLoader {

    /**
     * @param sPathToFile The absolute link to the file where the sources are
     * saved
     * @param sDelimiterType
     * @param default_result_threshold
     * @return the queries
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static Set<SearchQuery> LoadQueriesFromFile(String sPathToFile, String sDelimiterType, Integer default_result_threshold)
            throws FileNotFoundException, IOException {
        if (sDelimiterType == null || sDelimiterType.trim().isEmpty()) {
            sDelimiterType = "[*]{3}";
        }
        File fFile = new File(sPathToFile);
        if (!fFile.exists()) {
            throw new FileNotFoundException(fFile.getAbsolutePath() + " cannot be found.");
        }
        if (fFile.canRead()) {
            FileInputStream fstream = new FileInputStream(fFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.forName("utf-8")));
            String sLine;
            Set<SearchQuery> res = new HashSet();
            while ((sLine = br.readLine()) != null) {
                // if is not a comment line
                if (!sLine.startsWith("?")) {
                    String[] params = sLine.split(sDelimiterType);
                    SearchQuery sQcur = null;
                    if (params.length == 3) {
                        sQcur = new SearchQuery(
                                params[0],
                                params[1],
                                Integer.parseInt(params[2])
                        );
                    } else if (params.length == 2) {
                        if (default_result_threshold != null && default_result_threshold != 0) {
                            sQcur = new SearchQuery(
                                    params[0],
                                    params[1],
                                    default_result_threshold
                            );
                        } else {
                            sQcur = new SearchQuery(
                                    params[0],
                                    params[1]
                            );
                        }
                    } else if (params.length == 1) {
                        LOGGER.severe(String.format("'%s' does not have a language declaration, aborting", sLine));
                    }
                    if (sQcur != null) {
                        res.add(sQcur);
                    }
                }
            }
            in.close();
            return res;
        } else {
            LOGGER.log(Level.SEVERE, "Unable To Read From File {0}", fFile.getName());
            return null;
        }
    }
}

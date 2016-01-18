/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.utils;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class LangDetect {

    private volatile static LangDetect instance = null;

    public synchronized static LangDetect getInstance() {
        if (instance == null) {
            instance = new LangDetect();
        }
        return instance;
    }

    private LangDetect() {
        try {
            DetectorFactory.loadProfile("./profiles");
        } catch (LangDetectException ex) {
            Logger.getLogger(LangDetect.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String identifyLanguage(String text) {
        String language = "en";
        try {
            Detector detector = DetectorFactory.create();
            detector.append(text);
            language = detector.detect();
        } catch (LangDetectException e) {
            if (!e.getMessage().equals("no features in text")) {
                e.printStackTrace();
            }
        }
        return language;
    }

}

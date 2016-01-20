/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.demokritos.iit.crawlers.twitter.utils.langdetect;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * utilizes com.cybozu.labs.langdetect for statistical language detection
 *
 * @author George K. <gkiom@iit.demokritos.gr>
 */
public class CybozuLangDetect implements ILangDetect {

    private volatile static CybozuLangDetect instance = null;

    public synchronized static CybozuLangDetect getInstance() {
        if (instance == null) {
            instance = new CybozuLangDetect();
        }
        return instance;
    }

    private CybozuLangDetect() {
        try {
            DetectorFactory.loadProfile("./profiles");
        } catch (LangDetectException ex) {
            Logger.getLogger(CybozuLangDetect.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
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

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
        String language = null;
        if (text == null || text.trim().isEmpty()) {
            return language;
        }
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

    @Override
    public String identifyLanguage(String text, String default_on_fail) {
        String language = default_on_fail;
        if (text == null || text.trim().isEmpty()) {
            return language;
        }
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

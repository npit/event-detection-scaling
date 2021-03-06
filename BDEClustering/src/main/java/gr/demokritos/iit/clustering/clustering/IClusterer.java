package gr.demokritos.iit.clustering.clustering;

import org.apache.spark.api.java.JavaRDD;
import org.scify.newsum.server.model.datacollections.Articles;
import org.scify.newsum.server.model.structures.Topic;
import scala.Tuple4;

import java.util.Map;

public interface IClusterer {

    /**
     *
     * @param articles expects an RDD of <URL, title, text, timestamp>
     */
    void calculateClusters(Articles articles);
    Map<String,Topic> getArticlesPerCluster();

    }

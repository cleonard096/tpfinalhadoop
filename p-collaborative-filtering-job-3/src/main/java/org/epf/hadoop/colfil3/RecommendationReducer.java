package org.epf.hadoop.colfil3;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> recommendations = new ArrayList<>();

        for (Text val : values) {
            recommendations.add(val.toString());
        }


        recommendations.sort((a, b) -> {
            int commonA = Integer.parseInt(a.split(",")[1]);
            int commonB = Integer.parseInt(b.split(",")[1]);
            return Integer.compare(commonB, commonA); // Descending order
        });


        List<String> topRecommendations = recommendations.subList(0, Math.min(5, recommendations.size()));


        result.set(topRecommendations.toString());
        context.write(key, result);
    }
}


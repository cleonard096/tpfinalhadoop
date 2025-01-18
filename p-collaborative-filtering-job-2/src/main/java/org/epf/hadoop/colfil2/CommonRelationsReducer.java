package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CommonRelationsReducer extends Reducer<UserPair, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> commonFriends = new HashSet<>();
        boolean isDirectRelation = false;

        for (Text value : values) {
            if (value.toString().equals("direct")) {
                isDirectRelation = true;
            } else {
                commonFriends.add(value.toString());
            }
        }

        if (!isDirectRelation) {
            int commonFriendCount = commonFriends.size();
            result.set(String.valueOf(commonFriendCount));
            context.write(new Text(key.toString()), result);
        }
    }
}

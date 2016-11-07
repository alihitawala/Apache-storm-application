package storm.starter.q2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.*;

/**
 * Created by aliHitawala on 10/27/16.
 */
public class SlidingTwitterBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private List<String> hashTags;
    private static final int SAMPLE_HASHTAG_COUNT = 10;
    public static final String TAG = "SLIDING_TWITTER_BOLT_1";
    private final Set<String> sampledHashTags;
    private int friendsCount;

    public SlidingTwitterBolt(String[] hashTags) {
        this.hashTags = Arrays.asList(hashTags);
        this.friendsCount = 0;
        this.sampledHashTags = new HashSet<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        this.sampledHashTags.clear();
        ArrayList<Status> tempTweets = new ArrayList<>();
        for(Tuple tuple: inputWindow.get()) {
            if  (tuple.getSourceComponent().equals("hashTagGenerator")) {
                this.sampledHashTags.add((String) tuple.getValue(0));
            }
            else if (tuple.getSourceComponent().equals("friendsCountGenerator")) {
                this.friendsCount = (int) tuple.getValue(0);
            }
            else { // tweets
                Status tweet = (Status) (tuple.getValue(0));
                tempTweets.add(tweet);
            }
        }
        ArrayList<String> result = this.sampleTweets(tempTweets);
        //TODO write this to HDFS - sampled tweets
        this.collector.emit(new Values(result));
    }

    private ArrayList<String> sampleTweets(ArrayList<Status> tempTweets) {
        ArrayList<String> result = new ArrayList<>();
        for (Status tweet : tempTweets) {
            HashtagEntity[] hashtagEntities = tweet.getHashtagEntities();
            for (HashtagEntity hashtagEntity : hashtagEntities) {
                for (String sampleTag : this.sampledHashTags) {
                    String tag = hashtagEntity.getText().toLowerCase();
                    if (tweet.getUser().getFriendsCount() > this.friendsCount && (tag.contains(sampleTag) || sampleTag.contains(tag))) {
                        result.add(tweet.getText());
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweets"));
    }
}

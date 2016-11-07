package storm.starter.q2;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by aliHitawala on 10/28/16.
 */
public class HashTagGeneratorSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> hashTags;
    public static final int HASHTAG_GENERATION_DURATION = 1000;

    public HashTagGeneratorSpout(List<String> hashTags) {
        this.hashTags = hashTags;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashTags"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(HASHTAG_GENERATION_DURATION);
        collector.emit(new Values(this.getRandonHashTag()));
    }

    private String getRandonHashTag() {
        Collections.shuffle(this.hashTags);
        return this.hashTags.get(0);
    }
}

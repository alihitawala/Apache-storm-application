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

import java.util.Map;
import java.util.Random;

/**
 * Created by aliHitawala on 10/28/16.
 */
public class FriendsCountGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FriendsCountGenerator.class);
    private SpoutOutputCollector collector;
    private Random rand;

    public static final int FRIENDS_COUNT_GENERATION_DURATION = 2000;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("friendsCount"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(FRIENDS_COUNT_GENERATION_DURATION);
        collector.emit(new Values(rand.nextInt(11)));
    }
}

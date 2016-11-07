package storm.starter.q1;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by aliHitawala on 10/26/16.
 */
public class ParseTwitterBolt extends BaseRichBolt
{
    OutputCollector _collector;
    public static final String DELIMTIER = "\n||\n";

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        Status status = (Status) (tuple.getValue(0));
        String text = status.getText() + DELIMTIER;
        _collector.emit(new Values(text));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet"));
    }
}
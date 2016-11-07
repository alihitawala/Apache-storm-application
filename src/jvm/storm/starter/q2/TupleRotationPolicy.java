package storm.starter.q2;

import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

/**
 * Created by aliHitawala on 11/6/16.
 */
public class TupleRotationPolicy implements FileRotationPolicy{
    @Override
    public boolean mark(Tuple tuple, long offset) {
        System.out.println(tuple.getValue(0));
        if (tuple.getValue(0) instanceof String) {
            String value = (String) tuple.getValue(0);
            if (value.equals(Group8PartCQuestion2Topology.FILE_ROTATION))
                return true;
        }
        return false;
    }

    @Override
    public void reset() {

    }
}

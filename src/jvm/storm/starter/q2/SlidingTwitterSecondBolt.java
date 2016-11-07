package storm.starter.q2;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import storm.starter.EnglishStopWords;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by aliHitawala on 10/28/16.
 */
public class SlidingTwitterSecondBolt extends BaseRichBolt {
    private OutputCollector collector;
    private List<Bundle> bundles;
    private Map<String, Bundle> map;
    public static final String OUTPUT_DIR = "/home/ubuntu/storm_output/output_2/";
    public static final String DELIMTIER = "\n||\n";

    public SlidingTwitterSecondBolt() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.bundles = new ArrayList<>();
        this.map = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        ArrayList<String> tweets = (ArrayList<String>) (input.getValue(0));
        List<String> words = getAllNonStopWords(tweets);
        words = this.getTopHalfMostFreqWords(words);
        for (String tweet : tweets) {
            this.collector.emit("tweets", new Values(tweet + DELIMTIER));
        }
        for (String word : words) {
            this.collector.emit("words", new Values(word));
        }
        Utils.sleep(5000);
        this.collector.emit("tweets", new Values(Group8PartCQuestion2Topology.FILE_ROTATION));
        this.collector.emit("words", new Values(Group8PartCQuestion2Topology.FILE_ROTATION));
//        writeArrayToFile(tweets, OUTPUT_DIR + "tweets_" + counter + ".txt", true);
//        writeArrayToFile(words, OUTPUT_DIR + "topWords_"+ counter + ".txt", false);
    }

    private void writeArrayToFile(List<String> list, String fileName, boolean isTweet) {
        try {
            File file = new File(fileName);
            // if file doesnt exists, then create it
            if (!file.getParentFile().exists())
                file.getParentFile().mkdirs();
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            for (String s : list) {
                if (isTweet)
                    bw.write(s + "\n||\n");
                else
                    bw.write(s + "\n");
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<String> getTopHalfMostFreqWords(List<String> words) {
        for (String word : words) {
            if (this.map.containsKey(word)) {
                this.map.get(word).count++;
            } else {
                Bundle bundle = new Bundle(word);
                this.map.put(word, bundle);
            }
        }
        List<Bundle> temp = new ArrayList<>();
        for (String s : this.map.keySet()) {
            temp.add(this.map.get(s));
        }
        Collections.sort(temp);
        temp = temp.subList(0, temp.size()/2);
        List<String> result = new ArrayList<>();
        for (Bundle bundle : temp) {
            result.add(bundle.word);
        }
        return result;
    }

    private List<String> getAllNonStopWords(ArrayList<String> tweets) {
        Set<String> stopWords = EnglishStopWords.getInstance().getStopWords();
        List<String> result = new ArrayList<>();
        for (String tweet : tweets) {
            StringTokenizer tokenizer = new StringTokenizer(tweet);
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().toLowerCase().replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ");
                if (!stopWords.contains(word) && word.length() != 0) {
                    result.add(word);
                }
            }
        }
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("tweets", new Fields("word"));
        declarer.declareStream("words", new Fields("word"));
    }

    static class Bundle implements Comparable{
        private String word;
        private int count;

        public Bundle(String word) {
            this.word = word;
            this.count = 1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Bundle)) return false;

            Bundle bundle = (Bundle) o;
            return word.equals(bundle.word);

        }

        @Override
        public int hashCode() {
            return word.hashCode();
        }

        @Override
        public int compareTo(Object o) {
            Bundle b = (Bundle) o;
            return this.count > b.count ? -1 : (this.count < b.count ? 1 : 0);
        }
    }
}

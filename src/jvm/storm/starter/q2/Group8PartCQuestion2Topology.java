/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.starter.q2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.utils.Utils;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Group8PartCQuestion2Topology {
    public static final String FILE_ROTATION = "**DONE**";
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String isLocal = args[0];
        String consumerKey = args[1];
        String consumerSecret = args[2];
        String accessToken = args[3];
        String accessTokenSecret = args[4];
        String[] arguments = args.clone();
        String[] hashTags = Arrays.copyOfRange(arguments, 5, arguments.length);
        String[] keyWords = new String[0];
//        builder.setBolt("write", getHdfsBolt())
//                .shuffleGrouping("getText");

        Config conf = new Config();
        conf.setMessageTimeoutSecs(120);
        if (isLocal.equals("local")) {
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("twitter", new TwitterSpoutQuestion2(consumerKey, consumerSecret,
                    accessToken, accessTokenSecret, keyWords));
            builder.setSpout("hashTagGenerator", new HashTagGeneratorSpout(Arrays.asList(hashTags)));
            builder.setSpout("friendsCountGenerator", new FriendsCountGenerator());

            BaseWindowedBolt.Duration duration = new BaseWindowedBolt.Duration(30, TimeUnit.SECONDS);
            builder.setBolt("tweetFilter", new SlidingTwitterBolt(hashTags).withTumblingWindow(duration)).shuffleGrouping("twitter").shuffleGrouping("hashTagGenerator").shuffleGrouping("friendsCountGenerator");
            builder.setBolt("tweetParser", new SlidingTwitterSecondBolt()).shuffleGrouping("tweetFilter");
            builder.setBolt("hdfsTweets", getHdfsBolt("/user/ubuntu/storm_output_tweets/")).shuffleGrouping("tweetParser", "tweets");
            builder.setBolt("hdfsWords", getHdfsBolt("/user/ubuntu/storm_output_words/")).shuffleGrouping("tweetParser", "words");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter_top_words", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.shutdown();
        } else {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("twitter", new TwitterSpoutQuestion2(consumerKey, consumerSecret,
                    accessToken, accessTokenSecret, keyWords), 5).setNumTasks(10);
            builder.setSpout("hashTagGenerator", new HashTagGeneratorSpout(Arrays.asList(hashTags)));
            builder.setSpout("friendsCountGenerator", new FriendsCountGenerator());

            BaseWindowedBolt.Duration duration = new BaseWindowedBolt.Duration(30, TimeUnit.SECONDS);
            builder.setBolt("tweetFilter", new SlidingTwitterBolt(hashTags).withTumblingWindow(duration)).shuffleGrouping("twitter").shuffleGrouping("hashTagGenerator").shuffleGrouping("friendsCountGenerator");
            builder.setBolt("tweetParser", new SlidingTwitterSecondBolt()).shuffleGrouping("tweetFilter");
            builder.setBolt("hdfsTweets", getHdfsBolt("/user/ubuntu/storm_output_tweets/")).shuffleGrouping("tweetParser", "tweets");
            builder.setBolt("hdfsWords", getHdfsBolt("/user/ubuntu/storm_output_words/")).shuffleGrouping("tweetParser", "words");
            conf.setNumWorkers(5);
            StormSubmitter.submitTopology("twitter_top_words", conf, builder.createTopology());
        }
    }

    private static HdfsBolt getHdfsBolt(String path) {
//        Status status = (Status) (tuple.getValue(0));
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //Synchronize data buffer with the filesystem every 1000 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new TupleRotationPolicy();

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(path);

        // Instantiate the HdfsBolt
        return new HdfsBolt()
                .withFsUrl("hdfs://10.254.0.53:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
    }
}

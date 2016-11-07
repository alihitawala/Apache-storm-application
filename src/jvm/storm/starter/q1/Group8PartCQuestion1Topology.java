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

package storm.starter.q1;

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
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

public class Group8PartCQuestion1Topology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String isLocal = args[0].toLowerCase();
        String consumerKey = args[1];
        String consumerSecret = args[2];
        String accessToken = args[3];
        String accessTokenSecret = args[4];
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);

                
                
        Config conf = new Config();
        
        if (isLocal.equals("local")) {
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("twitter", new MyTwitterSampleSpout(consumerKey, consumerSecret,
                    accessToken, accessTokenSecret, keyWords));
            builder.setBolt("getText", new ParseTwitterBolt()).shuffleGrouping("twitter");
            builder.setBolt("write", getHdfsBolt())
                    .shuffleGrouping("getText");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter_write", conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.shutdown();
        } else {
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("twitter", new MyTwitterSampleSpout(consumerKey, consumerSecret,
                    accessToken, accessTokenSecret, keyWords), 5).setNumTasks(10);
            builder.setBolt("getText", new ParseTwitterBolt(), 2).shuffleGrouping("twitter").setNumTasks(4);
            builder.setBolt("write", getHdfsBolt(), 7)
                    .shuffleGrouping("getText").setNumTasks(14);
            conf.setNumWorkers(5);
            StormSubmitter.submitTopology("twitter_write", conf, builder.createTopology());
        }
    }

    private static HdfsBolt getHdfsBolt() {
//        Status status = (Status) (tuple.getValue(0));
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        //Synchronize data buffer with the filesystem every 1000 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/ubuntu/storm_output/");

        // Instantiate the HdfsBolt
        return new HdfsBolt()
                .withFsUrl("hdfs://10.254.0.53:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.ctc.wstx.util.StringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 1/16/13
 * Time: 5:32 PM
 */

public class FriendRecommendation {

    static final int N = 50000;

    static public class Line {
        int[] array = new int[N];

        public void set(Long index) {
            array[Integer.parseInt(index.toString())] = 1;
        }

        @Override
        public String toString() {
            return StringUtils.join(array,' ');
        }
    }

    static public class FriendCountWritable implements Writable {
        public Long user;
        public Long mutualFriend;

        public FriendCountWritable(Long user, Long mutualFriend) {
            this.user = user;
            this.mutualFriend = mutualFriend;
        }

        public FriendCountWritable() {
            this(-1L, -1L);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(user);
            out.writeLong(mutualFriend);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            user = in.readLong();
            mutualFriend = in.readLong();
        }

        @Override
        public String toString() {
            return " toUser: "
                    + Long.toString(user) + " mutualFriend: " + Long.toString(mutualFriend);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line[] = value.toString().split("\t");
            Long fromUser = Long.parseLong(line[0]);
            List<Long> toUsers = new ArrayList<Long>();

            if (line.length == 2) {
                StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
                while (tokenizer.hasMoreTokens()) {
                    Long toUser = Long.parseLong(tokenizer.nextToken());
                    toUsers.add(toUser);
                    context.write(new LongWritable(fromUser), new FriendCountWritable(toUser, -1L));
                }
            } else {
                context.write(new LongWritable(fromUser), new FriendCountWritable(-1L, -1L));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, FriendCountWritable, NullWritable, Text> {
        static long count = 0;
        @Override
        public void reduce(LongWritable key, Iterable<FriendCountWritable> values, Context context)
                throws IOException, InterruptedException {

            // key is the recommended friend, and value is the list of mutual friends
//            final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

            Line line = new Line();
            while (count<Long.parseLong(String.valueOf(key))) {
                context.write(NullWritable.get(),new Text(line.toString()));
                count ++;
            }
            count ++;
            for (FriendCountWritable val : values) {
                if (val.user != -1L)
                    line.set(val.user);
            }
            context.write(NullWritable.get(), new Text(line.toString()));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "FriendRecommendation");
        job.setJarByClass(FriendRecommendation.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FriendCountWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        FileSystem outFs = new Path("D:\\Hadoop\\output").getFileSystem(conf);
//        outFs.delete(new Path("D:\\Hadoop\\output"), true);

        FileInputFormat.addInputPath(job, new Path("D:\\Hadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Hadoop\\output"));

        job.waitForCompletion(true);
    }
}

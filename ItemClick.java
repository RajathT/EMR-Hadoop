import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ItemClick {

    public static class MiscUtils {

        /**
         * sorts the map by values. Taken from:
         * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
         */
        public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //LinkedHashMap will keep the keys in the order they are inserted
            //which is currently sorted on natural ordering
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }


    }

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String temp = value.toString();
            String[]comma_values = temp.split(",");
            String[]time = comma_values[1].split("T");
            String[]date = time[0].split("-");
            if(date[1].equals("04")){
                word.set(comma_values[2]);
                context.write(word, one);
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        //private IntWritable result = new IntWritable();
        private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //result.set(sum);
            //context.write(key, result);
            countMap.put(new Text(key), new IntWritable(sum));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "item click count");
        job.setJarByClass(ItemClick.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

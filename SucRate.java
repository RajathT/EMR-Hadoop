import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class SucRate {

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

    public static class ClickMap
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[]comma_values = value.toString().split(",");

            word.set(comma_values[2]);
            context.write(word, new Text("c"));
        }
    }
    public static class BuyMap
            extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[]comma_values = value.toString().split(",");

            word.set(comma_values[2]);
            context.write(word, new Text("b"));
        }
    }

    public static class IntSumReducer extends Reducer<Text,Text,Text,FloatWritable> {
        private FloatWritable result = new FloatWritable();

        private Map<Text, FloatWritable> countMap = new HashMap<Text, FloatWritable>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0, sum1 =0;
            for (Text val : values) {
                if (val.toString().equals("c"))
                    sum = sum + 1;
                else if (val.toString().equals("b"))
                    sum1 = sum1 + 1;
            }
            //result.set((float)sum1/sum);
            //context.write(key, result);
            countMap.put(new Text(key), new FloatWritable((float)sum1/sum));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, FloatWritable> sortedMap = MiscUtils.sortByValues(countMap);

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
        Job job = Job.getInstance(conf, "price-quant");
        job.setJarByClass(SucRate.class);
        job.setMapperClass(ClickMap.class);
        job.setMapperClass(BuyMap.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ClickMap.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BuyMap.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

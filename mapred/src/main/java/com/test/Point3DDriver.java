package com.test;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.bson.BasicBSONObject;

public class Point3DDriver {
    private static final transient Logger logger = Logger.getLogger("Point");

    static int count = 0;
    private static String cleanData = null;
    private static int cleanIndex = 0;
    //private static String ttttt = null;

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private Text newKey = new Text();  //key值
        private Text newValue = new Text();    //value值

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String str = "";
            String[] result;

            str = value.toString().replace("\n", " ").replace("}", "}\n").replace(">", ">\n");
            str = str.replaceAll("\\{\\{.+\\}\n\\}\n", "").replaceAll("\\<.+\\>\n", "");
            result = str.replaceAll("[^a-zA-Z]+", " ").split("[\\s]+");

            for (String tmp : result) {
                if (!tmp.equals("")) {

                    newKey.set(tmp+"#"+key);
                    newValue.set("1");
                    context.write(newKey, newValue);
                }
            }



        }
    }

    public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text val : values) {
                count += 1;
            }
            String[] tmp = key.toString().split("#");
            newKey.set(tmp[0]);
            newValue.set(tmp[1]+"#"+String.valueOf(count));
            context.write(newKey, newValue);
        }
    }

    public static class MyPartitioner extends Partitioner<Text,Text>{
        @Override
        public int getPartition(Text key, Text value, int i) {
            return (key.toString().split("#")[0].hashCode() & 0x7fffffff) % 2;
        }
    }

    //Reduce过程
    public static class MyReducer extends Reducer<Text, Text, Text, BSONWritable> {
        //private Text result = new Text();   //设定最终的输出结果

        //   word1   a#1
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str = "";
            int sum = 0;
            List<String[]> list = new ArrayList<String[]>();
            for (Text val : values) {
                String[] cont = val.toString().split("#");
                sum += Integer.parseInt(cont[1]);
                list.add(cont);
            }

            List<String> percent = new ArrayList<String>();
            for (int i = 0; i < list.size(); i++){
                String[] tmp = list.get(i);
                float n = Float.parseFloat(tmp[1]) / sum;
                percent.add(tmp[0]+" --> "+String.format("%.2f", n*100) +"%");
            }

            BasicBSONObject result = new BasicBSONObject();
            result.put("titles", percent);

            context.write(key, new BSONWritable(result));
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        logger.debug("scx@ main");
        try {
            Configuration conf = new Configuration();
//            String[] paths = new GenericOptionsParser(conf, args).getRemainingArgs();
//            if (paths.length < 1) {
//                throw new RuntimeException("usage <input> <output>");
//            }

            conf.set("xmlinput.key.start", "<title>");
            conf.set("xmlinput.key.end", "</title>");
            conf.addResource("mydefault.xml");
            //ttttt = conf.get("data.clean");
            conf.set("xmlinput.value.start", "<text xml:space=\"preserve\">");
            conf.set("xmlinput.value.end", "</text>");

            conf.setBoolean("mapreduce.output.map.compress", true);
            conf.setClass("mapreduce.map.output.compress.codec",GzipCodec.class, CompressionCodec.class);

            String[] input_paths = {"src/data/sample.xml"};
            //String[] input_paths = {"src/data/sample.xml"};
            String output_path = "output/";
            //String clean_path = "src/data/clean.txt";

            MongoConfigUtil.setOutputURI(conf, "mongodb://192.168.1.198:27017/wiki.search");
            SimpleDateFormat ymdhms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            Job job = Job.getInstance(conf, "Point3DDriver--" + ymdhms.format(new Date()));

            job.setJarByClass(Point3DDriver.class);
            //job.setNumReduceTasks(2);


            job.setMapperClass(MyMapper.class);
            job.setCombinerClass(MyCombiner.class);
            job.setPartitionerClass(MyPartitioner.class);
            job.setReducerClass(MyReducer.class);

            job.setInputFormatClass(MyInputFormat.class);
            //job.setOutputFormatClass(MongoOutputFormat.class);
            job.setOutputFormatClass(MyOutputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //job.setOutputValueClass(BSONWritable.class);

            for (String tmp : input_paths) {
                FileInputFormat.addInputPaths(job, tmp);
            }

            //FileInputFormat.addInputPaths(job, paths[0]);
//            if (paths.length == 2) {
//                cleanData = clean(paths[1]);
//            }

            //byte[] tmp = clean(clean_path);
            //cleanData = (new String(tmp, "utf-8")).substring(0,cleanIndex);


            FileOutputFormat.setOutputPath(job, new Path(output_path + System.currentTimeMillis()));// 整合好结果后输出的位置
            System.exit(job.waitForCompletion(true) ? 0 : 1);// 执行job  


        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static byte[] clean(String filepath) {
        byte[] filecontent = new byte[100];
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            Path inPath  = new Path(filepath);
            FSDataInputStream fsin = hdfs.open(inPath);
            fsin.seek(0);

            int available = fsin.available();

            byte ch = ' ';
            for(int i = 0; i < available; i++){
                ch = fsin.readByte();
                if(ch != 10) {
                    filecontent[cleanIndex++] = ch;
                }
                else{
                    filecontent[cleanIndex++] = '#';
                }
            }

//            String line = "";
//            line = br.readLine();
//            while (line != null) {
//                result.append(line).append("#");
//                line = br.readLine(); // 一次读入一行数据
//            }
        } catch (Exception e) {
            e.printStackTrace();
            return filecontent;
        }
        return filecontent;
    }
}  
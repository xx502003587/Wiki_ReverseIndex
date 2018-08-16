package com.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class MyRecordReader extends RecordReader<Text, Text> {
    private static final transient Logger logger = Logger.getLogger("MAP");

    public static final String KEY_VALUE_SEPERATOR = "mapreduce.input.mylinerecordreader.key.value.separator";

    public static final String KEY_START_TAG_KEY = "xmlinput.key.start";
    public static final String KEY_END_TAG_KEY = "xmlinput.key.end";

    public static final String VALUE_START_TAG_KEY = "xmlinput.value.start";
    public static final String VALUE_END_TAG_KEY = "xmlinput.value.end";

    private String titlePre;
    private String titleBack;
    private String textPre;
    private String textBack;
    private byte[] filecontent;
    private String content;

    private long start;
    private long end;
    private int titlePreIndex;
    private int entryCount;

    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private Text key = new Text();
    private Text value = new Text();


    public Class<Text> getKeyClass() {
        return Text.class;
    }

    public MyRecordReader(Configuration conf) throws IOException {
        String sepStr = conf.get(KEY_VALUE_SEPERATOR, "=");
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit fileSplit = (FileSplit) genericSplit;

        titlePre = context.getConfiguration().get(KEY_START_TAG_KEY).toString();
        titleBack = context.getConfiguration().get(KEY_END_TAG_KEY).toString();

        textPre = context.getConfiguration().get(VALUE_START_TAG_KEY).toString();
        textBack = context.getConfiguration().get(VALUE_END_TAG_KEY).toString();

        //获取分片起始、终止索引与长度
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        Path file = fileSplit.getPath();
        Configuration conf = context.getConfiguration();

        //初始化输入流
        FileSystem fs = file.getFileSystem(conf);
        fsin = fs.open(fileSplit.getPath());
        fsin.seek(start);

        //读取整个分片的数据
        filecontent = new byte[new Long(fileSplit.getLength()).intValue()];
        readFileContent(start, end);
        String encoding = "utf-8";
        content = new String(filecontent, encoding);
    }

    //读取整个分片的数据
    private void readFileContent(long start, long end) {
        try {
            for (int i = 0; i < end - start; i++) {
                filecontent[i] = fsin.readByte();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> solveKeyValueData() {
        Map<String, String> map = new HashMap<String, String>();
        try {
            int title_back_index = 0;
            int text_pre_index = 0;
            int text_back_index = 0;

            String title = "";
            String text = "";

            if ((titlePreIndex = content.indexOf(titlePre, titlePreIndex)) != -1) {
                //根据索引读取title属性值
                title_back_index = content.indexOf(titleBack, titlePreIndex + 5);

                //如果title后缀无法找到这表明当前词条还没有text内容
                if (title_back_index == -1) {
                    map.put("null", "null");
                    return map;
                }

                title = content.substring(titlePreIndex + 7, title_back_index);
                System.out.println(title);
                titlePreIndex = title_back_index + 8;

                //根据索引读取text属性值
                text_pre_index = content.indexOf(textPre, titlePreIndex);

                //如果text前缀无法找到这表明当前词条只有title没有text
                if (text_pre_index == -1) {
                    map.put("null", "null");
                    return map;
                }

                text_back_index = content.indexOf(textBack, text_pre_index + 27);
                //如果text后缀无法找到这表明当前词条text只有一部分
                if (text_back_index == -1) {
                    text = content.substring(text_pre_index + 27, content.length());
                    System.out.println(text);

                    map.put(title, text);
                    return map;
                }

                text = content.substring(text_pre_index + 27, text_back_index);
                System.out.println(text);
                titlePreIndex = text_back_index + 7;
                entryCount++;
                System.out.println("number is " + entryCount);
                map.put(title, text);
                return map;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        map.put("null","null");
        return map;
    }

    /**
     * Read key/value pair in a line.
     */
    public synchronized boolean nextKeyValue() throws IOException {
        Map<String, String> map = solveKeyValueData();
        for(String tmpKey : map.keySet()){
            if (tmpKey.equals("null")){
                return false;
            }else{
                key.set(tmpKey);
                value.set(map.get(tmpKey));
                return true;
            }
        }
        return true;
    }

    public Text getCurrentKey() {
        return key;
    }

    public Text getCurrentValue() {
        return value;
    }

    public float getProgress() throws IOException {

        return 0;
    }

    public synchronized void close() throws IOException {
    }

}

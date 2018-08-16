package com.test;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.File;
import java.io.FileInputStream;

public class Main {

    private static FSDataInputStream fsin;
    private static String fileName = "src/sample.xml";
    private static String title_pre = "<title>";
    private static String title_back = "</title>";
    private static String text_pre = "<text xml:space=\"preserve\">";
    private static String text_back = "</text>";
    private int title_pre_index = 0;

    public static void main(String[] args) {

        try {
            String encoding = "utf-8";
            File file = new File(fileName);
            Long filelength = file.length();

            byte[] filecontent = new byte[filelength.intValue()];

            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();

            String content = new String(filecontent, encoding);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void solve(String content) {
        try {
            int count = 0;
            int title_back_index = 0;

            int text_pre_index = 0;
            int text_back_index = 0;

            String title = "";
            String text = "";

            while ((title_pre_index = content.indexOf(title_pre, title_pre_index)) != -1) {
                //根据索引读取title属性值
                title_back_index = content.indexOf(title_back, title_pre_index + 5);
                title = content.substring(title_pre_index + 7, title_back_index);
                System.out.println(title);
                title_pre_index = title_back_index + 8;

                //根据索引读取text属性值
                text_pre_index = content.indexOf(text_pre, title_pre_index);

                //如果text前缀无法找到这表明当前词条只有title没有text
                if (text_pre_index == -1)
                    break;

                text_back_index = content.indexOf(text_back, text_pre_index + 27);
                //如果text后缀无法找到这表明当前词条text只有一部分
                if (text_back_index == -1) {
                    text = content.substring(text_pre_index + 27, content.length());
                    System.out.println(text);
                    System.out.println();
                    break;
                }

                text = content.substring(text_pre_index + 27, text_back_index);
                System.out.println(text);
                System.out.println();
                title_pre_index = text_back_index + 7;
                count++;
                System.out.println("------------------------------------");
            }
            System.out.println("匹配个数为" + count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

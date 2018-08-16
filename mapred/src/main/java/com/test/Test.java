package com.test;

import org.apache.hadoop.io.Text;

public class Test {
    public static void main(String[] args) {
        String s = "{{Redirect category shell|\n" +
                "{{R from move}}\n" +
                "{{R from CamelCase}}\n" +
                "{{R unprintworthy}}\n" +
                "}}";

        s = s.replace("\n"," ").replace("}","}\n").replace(">",">\n");
        String pattern = "\\{\\{.+\\}\n\\}\n";
        String pattern2 = "\\<.+\\>\n";
        s = s.replaceAll(pattern, "").replaceAll(pattern2, "");
        System.out.println(s);
        System.out.println("---------------------");
        String[] result = s.replaceAll("[^a-zA-Z]+", " ").split("[\\s]+");

        if (result.length > 0){
            for (String tmp : result) {
                if (!(tmp.equals(""))) {
                    System.out.print("###-->");
                    System.out.println(tmp);
                }
            }
        }else{
            System.out.print("null");
        }






    }
}

package com.hadoop;

import java.io.*;

/**
 * Created by linxiao on 2016/1/16.
 */
public class App {
    public static void main(String args[]) {

        try {
            BufferedReader bw = new BufferedReader(new FileReader("D:\\secureCRT\\user.txt"));
            String msg;
            while ((msg = bw.readLine()) != null ) {
                System.out.println(msg);
                String a [] = msg.split("\t");
                System.out.println(a[0]+", "+a[1]);
            }
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

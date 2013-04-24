package edu.knowitall.browser.util;

import java.io.*;
import java.util.*;

public class FileSorter {

    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        Map<String, String> map=new TreeMap<String, String>();
        String line="";
        while((line=reader.readLine())!=null){
                map.put(getField(line),line);
        }
        reader.close();
        FileWriter writer = new FileWriter(args[1]);
        for(String val : map.values()){
                writer.write(val);      
                writer.write('\n');
        }
        writer.close();
    }

    private static String getField(String line) {
        return line.split("\t")[0];//extract value you want to sort on
    }
}
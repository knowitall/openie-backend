package edu.knowitall.browser.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;
import java.util.TreeMap;

import edu.knowitall.browser.entity.Pair;

// Does the same thing as sorter but shuffles instead
// sortedmap uses Math.random() as keys now
public class FileRandomizer {

    public static void main(String[] args) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(args[0]));
        Map<Double, Pair<String, String>> map=new TreeMap<Double, Pair<String, String>>();
        String line="";
        while((line=reader.readLine())!=null){
                map.put(Math.random(), Pair.from(getField(line),line));
        }
        reader.close();
        FileWriter writer = new FileWriter(args[1]);
        for(Pair<String, String> keyValPair : map.values()){
                writer.write(keyValPair.two);      
                writer.write('\n');
        }
        writer.close();
    }

    private static String getField(String line) {
        return line.split("\t")[0];//extract value you want to sort on
    }
}
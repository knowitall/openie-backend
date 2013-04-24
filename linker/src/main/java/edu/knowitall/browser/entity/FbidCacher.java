package edu.knowitall.browser.entity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class FbidCacher {

	public static void main(String[] args) {
		try {
			Map<String, Pair<String, Integer>> fbid_to_line = new TreeMap<String, Pair<String, Integer>>();
			Map<String, String> titleMap = new TreeMap<String, String>();
			Map<String, String> cached = new TreeMap<String, String>();
			
			File input = new File("src/main/resources/output.fbid-prominence.sorted");
			BufferedReader reader = new BufferedReader(new FileReader(input));
			
			CandidateFinder.loadFbid(reader, fbid_to_line, titleMap, cached);
			
			writeKeyValuePairs(fbid_to_line, titleMap, cached);
			
			System.err.println("Done");
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Fatal initialization error while loading from fbid file");
				System.exit(1);
			}
	}

    private static void writeSerialized(
            Map<String, Pair<String, Integer>> fbid_to_line,
            Map<String, String> titleMap, Map<String, String> cached)
            throws IOException, FileNotFoundException {
        ObjectOutputStream fbid_to_line_out = new ObjectOutputStream(new FileOutputStream("src/main/resources/fbid_to_line.obj"));
        ObjectOutputStream titleMap_out = new ObjectOutputStream(new FileOutputStream("src/main/resources/titleMap.obj"));
        ObjectOutputStream cached_out = new ObjectOutputStream(new FileOutputStream("src/main/resources/cached.obj"));
        
        List<Pair<String, Pair<String, Integer>>> fbList = new LinkedList<Pair<String, Pair<String, Integer>>>();
        List<Pair<String, String>> tList = new LinkedList<Pair<String, String>>();
        List<Pair<String, String>> cList = new LinkedList<Pair<String, String>>();
        
        fbList.addAll(fromPairsToPairs(fbid_to_line.entrySet()));
        tList.addAll(toPairs(titleMap.entrySet()));
        cList.addAll(toPairs(cached.entrySet()));
        
        fbid_to_line_out.writeObject(fbList);
        titleMap_out.writeObject(tList);
        cached_out.writeObject(cList);
        
        fbid_to_line_out.flush();
        titleMap_out.flush();
        cached_out.flush();
        
        fbid_to_line_out.close();
        titleMap_out.close();
        cached_out.close();
    }
	
    private static void writeKeyValuePairs(
            Map<String, Pair<String, Integer>> fbid_to_line,
            Map<String, String> titleMap, Map<String, String> cached)
            throws IOException, FileNotFoundException {
        PrintStream fbid_to_line_out = new PrintStream(new FileOutputStream("src/main/resources/fbid_to_line.txt"));
        PrintStream titleMap_out = new PrintStream(new FileOutputStream("src/main/resources/titleMap.txt"));
        PrintStream cached_out = new PrintStream(new FileOutputStream("src/main/resources/cached.txt"));
        
        for (Entry<String, Pair<String, Integer>> e: fbid_to_line.entrySet()) {
            fbid_to_line_out.println(e.getKey() + "\t" + e.getValue().one + "\t" + e.getValue().two);
        }
        
        for (Entry<String, String> e: titleMap.entrySet()) {
            titleMap_out.println(e.getKey() + "\t" + e.getValue());
        }
        
        for (Entry<String, String> e: cached.entrySet()) {
            cached_out.println(e.getKey() + "\t" + e.getValue());
        }
        
        fbid_to_line_out.flush();
        titleMap_out.flush();
        cached_out.flush();
        
        fbid_to_line_out.close();
        titleMap_out.close();
        cached_out.close();
    }
    
	private static ArrayList<Pair<String, String>> toPairs(Set<Entry<String, String>> entries) {
		ArrayList<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(entries.size());
		for (Entry<String, String> entry : entries) {
			pairs.add(new Pair<String, String>(entry.getKey(), entry.getValue()));
		}
		return pairs;
		
	}
	
	private static ArrayList<Pair<String, Pair<String, Integer>>> fromPairsToPairs(Set<Entry<String, Pair<String, Integer>>> entries) {
		ArrayList<Pair<String, Pair<String, Integer>>> pairs = new ArrayList<Pair<String, Pair<String, Integer>>>(entries.size());
		for (Entry<String, Pair<String, Integer>> entry : entries) {
			pairs.add(new Pair<String, Pair<String, Integer>>(entry.getKey(), entry.getValue()));
		}
		return pairs;
		
	}

}

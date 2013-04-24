package edu.knowitall.browser.entity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class Indices {

    private static final String indices_file = "src/main/resources/indices.txt";

    private static final Pattern tabSplit = Pattern.compile("\t");

    private static Map<String, Integer> fbidIndexMap = null;

    private static synchronized Map<String, Integer> loadFbidIndices(String resourceName)
            throws IOException {

        System.err.println("Loading fbid indices");
        
        InputStream resource = Indices.class.getClassLoader().getResourceAsStream(resourceName);

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(resource)
        );

        Map<String, Integer> nakedMap = new TreeMap<String, Integer>();

        String line;
        while ((line = reader.readLine()) != null) {

            String[] split = tabSplit.split(line);
            if (split.length < 2) {
                System.err.println("Bad line: " + line);
                continue;
            } else {

                try {
                    int num = Integer.parseInt(split[1]);
                    nakedMap.put(split[0], num);
                } catch (NumberFormatException nfe) {
                    nfe.printStackTrace();
                }
            }

        }
        System.err.println("Done loading fbid indices");

        return Collections.unmodifiableMap(nakedMap);
    }

    public static synchronized Map<String, Integer> fbidToIndexMap() throws IOException {

        if (fbidIndexMap == null) {
            fbidIndexMap = loadFbidIndices(indices_file);
        }

        return fbidIndexMap;
    }

    /**
     * Convert and ToString()
     * 
     * @throws IOException
     */
    public static List<Integer> convertFbids(List<String> fbids) throws IOException {
        List<Integer> indices = new ArrayList<Integer>(fbids.size());
        for (String fbid : fbids) {
            if (fbid != null && fbidToIndexMap().containsKey(fbid)) {
                indices.add(fbidToIndexMap().get(fbid));
            }
        }
        return Collections.unmodifiableList(indices);
    }

}

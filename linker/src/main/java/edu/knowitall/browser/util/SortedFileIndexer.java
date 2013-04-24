package edu.knowitall.browser.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class SortedFileIndexer {

    // you'll need to make this a long[] for big files!
    public static int[] indexSortedFile(BufferedReader raf) throws IOException {
        List<Integer> indices = new LinkedList<Integer>();
        indices.add(0);
        //raf.seek(0L);
        int bytePosition = 0;
        String line;
        String lastLine = null;
        
        while ((line = raf.readLine()) != null) {
            bytePosition += line.getBytes().length + 1;
            indices.add(bytePosition);
            
            if (lastLine != null && lastLine.compareTo(line) > 0) {
                throw new RuntimeException("file not sorted");
            }
            lastLine = line;
        }
        
        int[] index = new int[indices.size()];
        int i = 0;
        for (Integer j : indices) {
            index[i] = j;
            i++;
        }
        
        return index;
    }
    
}

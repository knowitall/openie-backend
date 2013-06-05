package edu.knowitall.browser.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * does binary search within tab-delimited files where
 * the first column of the file is sorted (in java lexicographic order, bash sort wont work)
 * treats the second to nth columns as the "value" for the map.
 *
 * Should be pretty fast. Uses java NIO and a variable-size (user-specified) in-memory cache layer.
 * Caching has been observed to increase performance 3-5x for random lookups, and 10x or more for sequential lookups,
 * for cache size of about 10% of file size.
 *
 * Should be thread safe, but not tested.
 *
 * @author Rob
 *
 */
public class SortedFileMap {

    private static final int line_buffer_size = 100; // most lines should fit in this



    private final int cacheSize;

    private File file;
    private ThreadLocal<FileChannel> fileChannelLocal = new ThreadLocal<FileChannel>() {
        @Override
        public FileChannel initialValue() {
            FileInputStream fis;
            try {
                fis = new FileInputStream(file);
                return fis.getChannel();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    };

    private final ThreadLocal<CharsetDecoder> utf8DecoderLocal = new ThreadLocal<CharsetDecoder>() {
        @Override
        public CharsetDecoder initialValue() { return Charset.availableCharsets().get("UTF-8").newDecoder(); }
    };

    private final ThreadLocal<CacheMap> cacheLocal = new ThreadLocal<CacheMap>() {
        @Override
        public CacheMap initialValue() { return new CacheMap(cacheSize); }
    };

    private final int[] fileIndex;

    // A simple hack to optimize containsKey() followed by get() calls.
    private String lastKey = null;
    private String lastValue = null;

    // empty map constructor for singleton empty field.
    private SortedFileMap() {
        this.cacheSize = 0;
        this.fileIndex = new int[0];
    }

    public int cacheSize() { return cacheLocal.get().size(); }

    /**
     * How many lines of your file can you afford to hold in memory? This class lets you make a disk/memory tradeoff.
     * @param filename
     * @param maxCacheLines
     * @throws IOException
     */
    public SortedFileMap(String filename, int maxCacheLines) throws IOException {
        this(new File(filename), maxCacheLines);
    }
    
    public SortedFileMap(File file, int maxCacheLines) throws IOException {
        this.cacheSize = maxCacheLines;
        this.file = file;

        System.err.println("Indexing file for SortedFileMap: " + file.getCanonicalPath());
        BufferedReader indexReader = new BufferedReader(new FileReader(file));
        this.fileIndex = SortedFileIndexer.indexSortedFile(indexReader);
        indexReader.close();
        System.err.println("done indexing file for SortedFileMap: " + file.getCanonicalPath());
    }

    public boolean containsKey(String key) throws IOException {
        return get(key) != null;
    }

    public String get(String key) throws IOException {

        if (key.equals(lastKey)) return lastValue;

        lastKey = key;
        String lookup = binarySearch(key);
        if (lookup == null) {
            lastValue = null;
            return null;
        }
        else {
            int tab = lookup.indexOf("\t");
            String value = lookup.substring(tab+1); // this won't generalize
            lastValue = value;
            return value;
        }
    }

    private String binarySearch(String key) throws IOException {

        int low = 0;
        int high = fileIndex.length - 1;
        //System.err.println("looking for " + key);

        CacheMap localCache = cacheLocal.get();

        while (low <= high) {
            int mid = (low + high) / 2;
            String line = localCache.getOrElseUpdate(mid);
            // if you want to bypass the cache, just do this:
            //String line = getLineAtIndex(mid);
            if (line == null) {

                return null;
            }
            //System.err.println("trying: " + line);
            String currentKey = line.substring(0, line.indexOf("\t"));
            int comparison = key.compareTo(currentKey);

            if (comparison < 0) {
                high = mid-1;
            }
            else if (comparison > 0) {
                low = mid+1;
            }
            else {
                return line;
            }
        }
        return null;
    }

    private String getLineAtIndex(int index) throws IOException {
        FileChannel fileChannel = fileChannelLocal.get();
        fileChannel.position(fileIndex[index]);
        Reader internalReader = Channels.newReader(fileChannel, utf8DecoderLocal.get(), line_buffer_size);
        BufferedReader reader = new BufferedReader(internalReader, line_buffer_size);
        String line = reader.readLine();
        // can't close the reader.. or it will close the channel! Doesn't seem to be a leak problem.
        return line;
    }

    public static final SortedFileMap empty = new SortedFileMap();


    // args: sorted map file, test file, cache size
    public static void main(String[] args) throws IOException {
        SortedFileMap map = new SortedFileMap(args[0], Integer.parseInt(args[2]));
        BufferedReader inputReader = new BufferedReader(new FileReader(args[1]));
        //BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String inputLine;
        int failed = 0;
        int total = 0;
        long startTime = System.currentTimeMillis();
        while ((inputLine = inputReader.readLine()) != null) {
            String key = inputLine.substring(0, inputLine.indexOf("\t"));
            String lookup = map.get(key);
            total++;
            // failed is meaningless unless you modify the map to return the entire line from the file,
            // so that we can verify we got the right entry.
            if (lookup != null) {
                if (!lookup.startsWith(key)) {
                    failed++;
                    //System.out.println("Error   for ("+key+") Found: " + lookup);
                } else {
                    //System.out.println("Correct for ("+key+") Found: " + lookup);
                }
            }
            else {
                System.out.println("No result for (" + key + "), expected: " + inputLine);
                failed++;
            }

            if (total % 1000 == 0) {
                Double elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
                System.out.println(String.format("%d extractions in %.02f sec, %.06f sec/lookup, cache size: %d", total, elapsedSeconds, elapsedSeconds/total, map.cacheSize()));
            }

        }

        inputReader.close();
        System.out.println("Total failures: " + failed);
    }

    @SuppressWarnings("serial") // don't serialize this.
    private class CacheMap extends LinkedHashMap<Integer, String> {

        private final int cacheSizeLimit;

        public CacheMap(int cacheSizeLimit) {
            super(cacheSizeLimit*2+1, 0.5f, true);
            this.cacheSizeLimit = cacheSizeLimit;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
            if (this.size() > cacheSizeLimit) return true;
            else return false;
        }

        public String getOrElseUpdate(Integer index) throws IOException {
            if (!this.containsKey(index)) {
                String lookup = getLineAtIndex(index);
                this.put(index, lookup);
                return lookup;
            } else {
                return this.get(index);
            }
        }

    }
}
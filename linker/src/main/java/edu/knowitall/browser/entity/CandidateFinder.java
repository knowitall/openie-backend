package edu.knowitall.browser.entity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.actors.threadpool.Arrays;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import edu.knowitall.browser.util.SortedFileMap;

/**
 * A supertype for candidate generators derived from Tom's original freebase linker.
 * This supertype contains the very large dictionaries that Tom's code depended on, as
 * static, synchronized, and lazily initialized SortedFileMaps. 
 *  
 * @author Rob
 *
 */
abstract class CandidateFinder {

	protected static Pattern splitPattern = Pattern.compile("\t");
	protected static Pattern spacePattern = Pattern.compile(" ");
	protected static Pattern commaSpacePattern = Pattern.compile(",|\\s");
	protected static Pattern exactMatch = Pattern.compile("(.*)\\s\\(.*\\)");
	
	private static SortedFileMap fbidToTitleInlinksRAW = SortedFileMap.empty;
	private static SortedFileMap titleMapRAW = SortedFileMap.empty;
	private static SortedFileMap cachedRAW = SortedFileMap.empty;

	private static final Integer FileSearch_CacheSize = 250000; // 300000 is about 10% of the full files' sizes, 

	private SortedFileMap fbidToTitleInlinks(String inputFile) 	throws FileNotFoundException, IOException, ClassNotFoundException {
		if (fbidToTitleInlinksRAW != SortedFileMap.empty)
			return fbidToTitleInlinksRAW;
		synchronized (fbidToTitleInlinksRAW) {
			if (fbidToTitleInlinksRAW == SortedFileMap.empty) {
				fbidToTitleInlinksRAW = loadFbidToTitleInlinks(inputFile);
			}
			return fbidToTitleInlinksRAW;
		}

	}

	private SortedFileMap titleMap(String inputFile)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		if (titleMapRAW != SortedFileMap.empty)
			return titleMapRAW;
		synchronized (titleMapRAW) {
			if (titleMapRAW == SortedFileMap.empty) {
				titleMapRAW = loadTitleMap(inputFile);
			}
			return titleMapRAW;
		}
	}

	private SortedFileMap cached(String inputFile)
			throws FileNotFoundException, IOException, ClassNotFoundException {
		if (cachedRAW != SortedFileMap.empty)
			return cachedRAW;
		synchronized (cachedRAW) {
			if (cachedRAW == SortedFileMap.empty) {
				cachedRAW = loadCachedMap(inputFile);
			}
			return cachedRAW;
		}
	}

	private final String fbidTitleFile;
	
	// original perl scripts called this "fbid_to_line"
	public SortedFileMap fbidToTitleInlinks() throws FileNotFoundException, IOException, ClassNotFoundException {
        
	    return fbidToTitleInlinks(fbidTitleFile);
    }
	
	private final String titleMapFile;
	
	public SortedFileMap titleMap() throws FileNotFoundException, IOException, ClassNotFoundException {
	    
	    return titleMap(titleMapFile);
	}
	
	private final String cachedMapFile;
	
	public SortedFileMap cached() throws FileNotFoundException, IOException, ClassNotFoundException {
	    
	    return cached(cachedMapFile);
	}
	
	/**
	 * Uses the default base dir and default support file locations.
	 */
	public CandidateFinder() {
		this(DefaultBaseDir);
	}
	
	/**
	 * Uses a given base dir for support files
	 */
	public CandidateFinder(String baseDir) {
	    this.fbidTitleFile = baseDir + "/" + fbidToTitleInlinksDefaultFile;
	    this.titleMapFile  = baseDir + "/" + titleMapDefaultFile;
	    this.cachedMapFile = baseDir + "/" + cachedMapDefaultFile;
	}
	
	/**
	 * Allows individually specified support file locations
	 * @param fbidTitleFile
	 * @param titleMapFile
	 * @param cachedMapFile
	 */
	public CandidateFinder(String fbidTitleFile, String titleMapFile, String cachedMapFile) {
	    this.fbidTitleFile = fbidTitleFile;
	    this.titleMapFile  = titleMapFile;
	    this.cachedMapFile = cachedMapFile;
	}

	public abstract List<Pair<String, Double>> linkToFbids(String arg) throws FileNotFoundException,
	IOException, ClassNotFoundException;
	
	/** Does not attach types */
	public List<Pair<Entity, Double>> linkToEntities(String arg) throws FileNotFoundException,
		IOException, ClassNotFoundException {

		List<Pair<String, Double>> fbids = linkToFbids(arg);
		Function<Pair<String, Double>, Pair<Entity, Double>> fbidLookupFn = new Function<Pair<String, Double>, Pair<Entity, Double>>() {
			public Pair<Entity, Double> apply(Pair<String, Double> fbidCprobPair) {
				try {
				    String fbid = fbidCprobPair.one;
				    double cprob = fbidCprobPair.two;
					Pair<String, Integer> titleInlinks = getTitleInlinks(fbid);
					Entity entity = new Entity(titleInlinks.one, fbid);
					return new Pair<Entity, Double>(entity, cprob);
				} catch (Exception e) {
					throw new RuntimeException(e);
				} // hi java
			}
		};
		
		return Lists.transform(fbids, fbidLookupFn);
	}

	public synchronized Pair<String, Integer> getTitleInlinks(String fbid)
			throws FileNotFoundException, IOException, ClassNotFoundException {

	    String lookup = fbidToTitleInlinks().get(fbid);
	    if (lookup == null) {
	        return null;
	    }
	    else {
	        String[] split = lookup.split("\t");
	        return new Pair<String, Integer>(split[0], Integer.parseInt(split[1]));
	    }
	    
	}


	@SuppressWarnings("unchecked")
	static void loadFbid(BufferedReader reader,
			Map<String, Pair<String, Integer>> fbid_to_line,
			Map<String, String> titleMap, Map<String, String> cached)
			throws IOException {
		String line;
		System.err.println("Loading fbid");
		while ((line = reader.readLine()) != null) {

			String[] split = splitPattern.split(line);
			String fbid = split[0];
			if (fbid.length() < 3)
				continue;
			fbid = fbid.substring(3);
			String inlinks = split[1];
			String title = split[2];
			Set<String> allWords = new HashSet<String>();
			allWords.addAll(Arrays.asList(spacePattern.split(title)));

			Set<String> usedAlready = new HashSet<String>();

			for (String word : allWords) {
				if (usedAlready.contains(word)) {
					// do nothing for now
				} else {
					if (!cached.containsKey(word)) {
						cached.put(word, fbid);
					} else {
						cached.put(word, cached.get(word) + "\t" + fbid);
					}
				}
				usedAlready.add(word);
			}

			Integer numInlinks;
			try {
				numInlinks = Integer.parseInt(inlinks);
			} catch (Exception e) {
				e.printStackTrace();
				numInlinks = 1;
			}

			// fbid_to_line.put(fbid, "hw\t"+fbid+"\t"+inlinks+"\t"+title);
			fbid_to_line
					.put(fbid, new Pair<String, Integer>(title, numInlinks));

			Matcher matcher = exactMatch.matcher(title);

			if (matcher.matches()) {
				String tmp = matcher.group(1);
				if (titleMap.containsKey(tmp)) {
					// nothing for now
				} else {
					titleMap.put(title, fbid);
				}

			}
			if (!titleMap.containsKey(title)) {
				titleMap.put(title, fbid);
			}
		}
	}
	
	public static final String DefaultBaseDir = "/scratch/browser-freebase";
	
	private static synchronized SortedFileMap loadFbidToTitleInlinks(String inputFile)
			throws IOException, FileNotFoundException, ClassNotFoundException {
		System.err.println("Loading Fbid to Title/Inlink Map (fbid_to_line)");
		
		return new SortedFileMap(inputFile, FileSearch_CacheSize);
	}

	private static final String fbidToTitleInlinksDefaultFile = "fbid_to_line.sorted";
	
	private static synchronized SortedFileMap loadTitleMap(String inputFile) throws IOException,
			FileNotFoundException, ClassNotFoundException {
		System.err.println("Load entity name to fbid map \"titleMap\"");
		
		return new SortedFileMap(inputFile, FileSearch_CacheSize);
	}

	private static final String titleMapDefaultFile = "titleMap.sorted";
	
	private static synchronized SortedFileMap loadCachedMap(String inputFile) throws IOException,
			FileNotFoundException, ClassNotFoundException {
		System.err
				.println("Loading \"cached\" token to fbid candidate map");
		
		return new SortedFileMap(inputFile, FileSearch_CacheSize);
	}

	private static final String cachedMapDefaultFile = "cached.sorted";
	
	protected static void println(String line) {
		System.out.println(line);
	}
}

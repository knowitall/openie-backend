package edu.knowitall.browser.entity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Contains a translated version of the candidate generation phase from Tom's original Perl
 * entity linker. Just the string-matching code has been separated out from the supporting code,
 * which now resides in the supertype.
 * 
 * @author Rob
 */
public class StringMatchCandidateFinder extends CandidateFinder {

  private int senses = 5;
  
  public StringMatchCandidateFinder(File basePath) {
    super(basePath);
  }
  
  /**
   * Computes the "string match level between the two args, which is an integer from 1 to 5. 5 is an
   * exact match, while 1 means the entity has many more words than the argument. Note that using
   * the algorithm in this class, the set of words in the entity is always a superset of the set of
   * words in the arg.
   */
  private int computeStringMatchLevel(String title, String arg) {
    if (arg.equalsIgnoreCase(title)) {
      return 5;
    } else {
      int wordDiff = title.split(" ").length - arg.split(" ").length;
      int stringMatchLevel = 4 - wordDiff;
      return Math.max(1, stringMatchLevel);
    }
  }

  @Override
  public List<Pair<String, Double>> linkToFbids(String arg) throws FileNotFoundException,
      IOException, ClassNotFoundException {
    if (arg == null) throw new NullPointerException("arg is null!");

    List<Pair<String, Double>> returnFbids = new LinkedList<Pair<String, Double>>();

    int exactmatched = 0;

    // See if there is an exact match for arg.
    if (titleMap().containsKey(arg)) {
      String fbid = titleMap().get(arg);
      int stringMatchLevel = computeStringMatchLevel(arg, arg);
      returnFbids.add(new Pair<String, Double>(fbid, (double) stringMatchLevel));
      exactmatched = 1;
    }
    // 134
    String[] eieParts = spacePattern.split(arg);
    String firstWord = eieParts[0];

    // If arg starts with "the", try removing it.
    if (firstWord.toLowerCase().equals("the")) {
      if (arg.length() >= 4)
        arg = arg.substring(4);
      else
        arg = "";
    }
    // 144
    // if there wasn't an exact match, try again if there was a leading
    // "the"
    if (exactmatched == 0 && titleMap().containsKey(arg)) {
      String fbid = titleMap().get(arg);
      int stringMatchLevel = computeStringMatchLevel(arg, arg);
      returnFbids.add(new Pair<String, Double>(fbid, (double) stringMatchLevel));
    }

    // if the arg doesn't contain any spaces (is a single token), try
    // looking it up
    // in "cached", a one-to-many string to fbid map that returns candidates
    // instead of exact matches.
    if (!spacePattern.matcher(arg).matches() && cached().containsKey(arg)) {
      String cachedResult = cached().get(arg);
      String[] cachedParts;
      if (cachedResult == null)
        cachedParts = new String[0];
      else
        cachedParts = splitPattern.split(cachedResult);

      int hitcount = 0;
      for (String cachedPart : cachedParts) {
        // TODO: fix this
        String titleAndInlinks = fbidToTitleInlinks().get(cachedPart);
        String[] parts = titleAndInlinks.split("\t");
        String title = parts[0];
        int stringMatchLevel = computeStringMatchLevel(title, arg);
        returnFbids.add(new Pair<String, Double>(cachedPart, (double) stringMatchLevel));
        hitcount++;
        if (hitcount >= senses) break;
      }

    } else {
      // break arg into tokens, splitting on whitespace and commas.
      String[] components = commaSpacePattern.split(arg);
      List<String> lists = new LinkedList<String>();
      boolean fail = false;
      for (String component : components) {
        if (cached().containsKey(component)) {
          lists.add(cached().get(component));

        } else if (component.length() > 2) {
          String newCap =
              component.substring(0, 1).toUpperCase() + component.substring(1).toLowerCase();
          if (cached().containsKey(newCap)) {
            lists.add(cached().get(newCap));
          } else {
            newCap = newCap.toLowerCase();
            if (cached().containsKey(newCap)) {
              lists.add(cached().get(newCap));
            } else {
              fail = true;
            }
          }
        }
      }

      if (fail) {
        if (exactmatched == 1) System.err.println("Throwing away" + arg);
        return Collections.emptyList();
      } else {

        Map<String, Integer> list_memberships = new HashMap<String, Integer>();
        List<String> candidates = new LinkedList<String>();
        int necessary = 0;
        for (String list : lists) {
          if (list == null || list.isEmpty()) continue;
          ++necessary;
          String[] listparts = splitPattern.split(list);
          if (listparts == null || listparts.length == 0) continue;
          for (String listPart : listparts) {
            if (!list_memberships.containsKey(listPart)) {
              candidates.add(listPart);
              list_memberships.put(listPart, 0);
            }
            list_memberships.put(listPart, list_memberships.get(listPart) + 1);
          }
        }

        int hitcount = 0;

        for (String candidate : candidates) {
          if (list_memberships.containsKey(candidate)
              && list_memberships.get(candidate) == necessary) {
            // TODO: fix this.
            String titleAndInlinks = fbidToTitleInlinks().get(candidate);
            String[] parts = titleAndInlinks.split("\t");
            String title = parts[0];
            int stringMatchLevel = computeStringMatchLevel(title, arg);
            returnFbids.add(new Pair<String, Double>(candidate, (double) stringMatchLevel));
            hitcount++;
            if (hitcount >= senses) break;
          }
        }
      }

    }
    return returnFbids;
  }

  @Override
  public boolean hasCandidates(String arg) {
    try {
      return !linkToFbids(arg).isEmpty();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    File basePath = new File(args[0]);
    CandidateFinder candidateFinder = new StringMatchCandidateFinder(basePath);
    
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    while (!stdin.ready()) {
      String arg = stdin.readLine();
      List<Pair<Entity, Double>> entityScores = candidateFinder.linkToEntities(arg);
      for (Pair<Entity, Double> entityScore : entityScores) {
        Entity entity = entityScore.one;
        System.out.println(
          String.format("%s\t%s\t%s\t%f", arg, entity.name, entity.fbid, entityScore.two)
        );
      }
    }
  }
}

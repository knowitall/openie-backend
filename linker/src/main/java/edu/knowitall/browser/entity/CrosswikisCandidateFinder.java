package edu.knowitall.browser.entity;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

import edu.knowitall.browser.util.CrosswikisHandler;
import edu.knowitall.browser.util.CrosswikisHandler.CrosswikisData;

public class CrosswikisCandidateFinder extends CandidateFinder {
  private CrosswikisHandler cwHandler;

  // Parameters.
  private double cprobCutoff;
  private int countCutoff;

  public CrosswikisCandidateFinder(File basePath) {
    // Default parameters chosen after tuning experiments.
    this(basePath, 0.5, 500); // High precision setting.
    // this(basePath, 0.01, 10); // High recall settings.
  }

  public CrosswikisCandidateFinder(File basePath, double cprobCutoff, int countCutoff) {
    super(basePath);
    cwHandler = new CrosswikisHandler(Constants.derbyDbUrl(Constants.crosswikisDbPath(basePath)));
    this.cprobCutoff = cprobCutoff;
    this.countCutoff = countCutoff;
  }

  private String getTitle(String articleString) {
    return articleString.replaceAll("_", " ");
  }

  /**
   * Given a string, find a list of candidate entities (Freebase IDs) that it
   * could be linked to.
   * 
   * @param arg The string we want to link.
   * @return A list of FBIDs that are candidate entities for the given string.
   */
  @Override
  public synchronized List<Pair<String, Double>> linkToFbids(String arg)
      throws FileNotFoundException, IOException, ClassNotFoundException {
    if (arg == null) {
      throw new RuntimeException("Error: arg in linkToFbids is null.");
    }

    List<CrosswikisData> results = cwHandler.getEntityDistribution(arg, cprobCutoff, countCutoff);

    List<Pair<String, Double>> returnFbids = new LinkedList<Pair<String, Double>>();
    for (CrosswikisData result : results) {
      String title = getTitle(result.getArticle());
      String fbid = titleMap().get(title);
      double cprob = result.getCprob();
      if (fbid != null) {
        // Normalize the score to be between 1 - 5, as in Tom's
        // algorithm. Change later?
        returnFbids.add(new Pair<String, Double>(fbid, 1 + (cprob * 4)));
      }
    }

    return returnFbids;
  }

  @Override
  public boolean hasCandidates(String arg) {
    return cwHandler.hasAnchor(arg);
  }

  /**
   * Main method for testing the CrosswikisCandidateFinder.
   * 
   * @param args
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws FileNotFoundException
   */
  public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException,
      IOException {
    File basePath = new File(args[0]);

    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    String arg;

    CandidateFinder candidateFinder = new CrosswikisCandidateFinder(basePath); 
    while ((arg = stdin.readLine()) != null) {
      List<Pair<Entity, Double>> entities = candidateFinder.linkToEntities(arg);
      List<Entity> entitiesOnly = new LinkedList<Entity>();
      for (Pair<Entity, Double> pair : entities) {
        entitiesOnly.add(pair.one);
      }
      String entitiesFmt =
          StringUtils.join(Lists.transform(entitiesOnly, Entity.serializeFunction), "\t");
      System.out.println(String.format("%s\t%s", arg, entitiesFmt));
    }
  }
}

package edu.knowitall.browser.entity;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.knowitall.browser.util.DerbyDb;

public class Indices extends DerbyDb {
  public Indices(String entityLinkingDerbyUrl) {
      super(entityLinkingDerbyUrl);
  }
  
  public List<Integer> getIndices(List<String> fbids) {
      List<Integer> indices = new ArrayList<Integer>();
      for (String fbid : fbids) {
          PreparedStatement statement = null;
          try {
              statement = connection.prepareStatement(
                  "SELECT index FROM fbid_indices WHERE fbid=?"
              );
              statement.setString(1, fbid);
              ResultSet results = statement.executeQuery();
              while (results.next()) {
                  int index = results.getInt(1);
                  indices.add(index);
              }
              statement.close();
          } catch (SQLException e) {
              throw new RuntimeException("Exception with FBID indices lookup.", e);
          }
      }
      return indices;
  }
}
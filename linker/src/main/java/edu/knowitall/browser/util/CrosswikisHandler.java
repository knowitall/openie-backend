package edu.knowitall.browser.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

public class CrosswikisHandler extends DerbyDb {
    public CrosswikisHandler(String url) {
        super(url);
    }

    /**
     * Get the lower-cased normalized version of the given string. Canonicalizes
     * UTF-8 characters, removes diacritics, lower-cases the UTF-8 and throws
     * out all ASCII-range characters that are not alphanumeric.
     * 
     * @param string
     * @return
     */
    public static String getLnrm(String string) {
        String lnrm = Normalizer.normalize(string, Normalizer.Form.NFD);
        lnrm = lnrm.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        lnrm = lnrm.toLowerCase();
        lnrm = lnrm.replaceAll("[^\\p{Alnum}]+", "");
        return lnrm;
    }

    /**
     * Returns true if the given anchor string exists in the Crosswikis data.
     * 
     * @param anchor
     * @return
     */
    public boolean hasAnchor(String anchor) {
        PreparedStatement countStatement = null;
        try {
            countStatement =
                connection.prepareStatement("SELECT count(*) FROM crosswikis WHERE anchor=?");
            countStatement.setString(1, getLnrm(anchor));
            ResultSet resultSet = countStatement.executeQuery();
            resultSet.next();
            int count = resultSet.getInt(1);
            return count > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Error with Crosswikis lookup in hasAnchor.", e);
        }
    }

    /**
     * Gets the distribution of entities given the string to link from the
     * Crosswikis data.
     * 
     * @param string The string to link.
     * @return A list of {@link CrosswikisData} objects associated with the
     *         given string.
     */
    public List<CrosswikisData> getEntityDistribution(String string, double cprobCutoff,
            int countCutoff) {
        ArrayList<CrosswikisData> results = new ArrayList<CrosswikisData>();

        PreparedStatement selectStatement = null;
        try {
            selectStatement = connection.prepareStatement(
                "SELECT entity, cprob, count "
              + "FROM crosswikis "
              + "WHERE anchor=? and cprob >= ? and count >= ?"
            );
            selectStatement.setString(1, getLnrm(string));
            selectStatement.setDouble(2, cprobCutoff);
            selectStatement.setDouble(3, countCutoff);
            ResultSet resultSet = selectStatement.executeQuery();
            while (resultSet.next()) {
                String entity = resultSet.getString(1);
                double cprob = resultSet.getDouble(2);
                int count = resultSet.getInt(3);
                CrosswikisData data = new CrosswikisData(string, entity, cprob, count);
                results.add(data);
            }
            selectStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error with Crosswikis lookup in getEntityDistribution.", e);
        }

        return results;
    }

    /**
     * Container for Crosswikis data.
     */
    public class CrosswikisData {
        private String anchor;
        private String article;
        private double cprob;
        private int count;

        public CrosswikisData(String anchor, String article, double cprob, int count) {
            this.anchor = anchor;
            this.article = article;
            this.cprob = cprob;
            this.count = count;
        }

        public String getAnchor() {
            return anchor;
        }

        public String getArticle() {
            return article;
        }

        public double getCprob() {
            return cprob;
        }

        public int getCount() {
            return count;
        }

        public String toString() {
            return new StringBuilder(anchor).append("\t").append(article).append("\t")
                    .append(cprob).append("\t").append(count).append("\t").toString();
        }
    }
}

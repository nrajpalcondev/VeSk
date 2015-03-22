package conductor.verifier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

/**
 * This {@link Verifier} implementation should actually check that all the rows in the database's keyword_url_facts
 * table were successfully migrated to S3, according to the rules laid out in the README.md file. See
 * {@link PrintingVerifier} for some clues on how to interact with the S3 report and the database.
 *
 * YOUR IMPLEMENTATION GOES BELOW...
 */
@Component("productionVerifier")
public class ProductionVerifier extends AbstractVerifier {

	/**
     * Converts the current row of the ResultSet to a String in CSV format.
     * @param resultSet the ResultSet to render
     * @return the CSV String
     * @throws SQLException
     */

	Hashtable<String, String> store_mySQL = new Hashtable<String, String>();
	Hashtable<String, String> store_S3 = new Hashtable<String, String>();
	
    private String resultSetToString(final ResultSet resultSet) throws SQLException {
        return resultSet.getLong("keyword_id")
        		+ "," + "STANDARD_LINK"
                + "," + resultSet.getString("url")
                + "," + resultSet.getString("domain_name")
                + "," + resultSet.getInt("google_search_rank")
                + "," + resultSet.getInt("google_page_rank");
    }
    
    @Override
    public void verify(final VerificationContext context) {
        /* Get the clients for S3 and the database. */

        final AmazonS3Client s3Client = getS3Client();
        final JdbcTemplate jdbcTemplate = getJdbcTemplate();

        /* Create a reader for the S3 data. */
        final S3ObjectInputStream strm = s3Client.getObject("searchlight-reports", "serp_items_report").getObjectContent();
        final BufferedReader s3Reader = new BufferedReader(new InputStreamReader(strm));

        /* Execute a simple query against the legacy reporting database. The {@link ResultSetExtractor} is used to
        process the entire {@link ResultSet}. Note that there are other ways of stepping through a ResultSet using
        JdbcTemplate, such as using a {@link RowMapper}.
         */
         
        // Using the query with all the joins to match S3 and mySQL
         jdbcTemplate.query("SELECT keyword_id, urls.url,domains.domain_name,google_search_rank,google_page_rank FROM keyword_url_facts"
         					+ " JOIN domains ON domains.id = keyword_url_facts.domain_id"
        		 			+ " JOIN urls ON urls.id = keyword_url_facts.url_id"
        		 			+ " JOIN keywords ON keywords.id = keyword_url_facts.keyword_id", new ResultSetExtractor() {

             @Override
             public Void extractData(final ResultSet resultSet) throws SQLException {
                 try {
                	 // Here I match the format for S3 and mySQL for ease of comparison
                     while (true) {
                    	 final String mySqlLine = resultSet.next() ? resultSetToString(resultSet) : null;
                    	 
                    	 if (mySqlLine == null) break;
                    	 
                    	 //storing the output from the SQL to a hashtable using the key as "keyword_id-searchrank-pagerank"
                    	 if(((Integer.parseInt(mySqlLine.split(",")[4]) != 0)) && ((Integer.parseInt(mySqlLine.split(",")[5]) != 0)))
                    		 store_mySQL.put(mySqlLine.split(",")[2]+"-"+mySqlLine.split(",")[4]+"-"+mySqlLine.split(",")[5], mySqlLine);
                     }
                     
                    while (true) {
                    	 final String s3Line = s3Reader.readLine();
                    	 
                    	//storing the output from the S3 to a hashtable using the key as "keyword_id-searchrank-pagerank"
                    	 if (s3Line == null) break;
                         store_S3.put(s3Line.split(",")[2]+"-"+s3Line.split(",")[4]+"-"+s3Line.split(",")[5], s3Line);
                     }
                   
                    
              //   looping all the keys from hash table to find a match between the 2 data source. While doing this I remove all the keys 
              //   so that it is not repeated when doing the diff between S3 and mySQL later
                   Enumeration mySQLe = store_mySQL.keys();
                   System.out.println("************Missing/Unmatched record in S3 (Comparing SQL Versus S3)*************");
                   
                   while (mySQLe.hasMoreElements()){
                	   String key = mySQLe.nextElement().toString();

                	   if(!(store_mySQL.get(key)).equals(store_S3.get(key))){
                		    System.out.println(store_mySQL.get(key)+"*****"+store_S3.get(key));
                	   }
               		   store_mySQL.remove(key); // removing key so that I don't count the key again in the next step
               		   store_S3.remove(key); // removing key so that I don't count the key again in the next step
                   }
                   
              //   looping all the left over keys from hash table to find a match between the 2 data source. 
                   
                   Enumeration S3e = store_S3.keys();
                   System.out.println("\n\n***Additional record found in S3 that is not in MySQL(Comparing SQL Versus S3)***");
                   while (S3e.hasMoreElements()){
                	   String key = S3e.nextElement().toString();
                	   System.out.println(store_mySQL.get(key)+"*****"+store_S3.get(key));
                    }
                  } catch (final Exception e) {
                     System.out.println(e);
                 }
                 return null;
             }
         });
    }
}
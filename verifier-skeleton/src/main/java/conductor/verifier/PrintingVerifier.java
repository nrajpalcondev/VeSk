package conductor.verifier;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A toy {@link Verifier} implementation that prints side-by-side the records from S3 and a "SELECT *" query from the
 * MySQL reporting database.
 *
 * This class doesn't actually verify anything about the success of the migration; all it does is print the data.
 * Feel free to modify it to see how the various components work and to explore different interactions with the
 * database.
 */
@Component("printingVerifier")
public class PrintingVerifier extends AbstractVerifier {

    /**
     * Converts the current row of the ResultSet to a String in CSV format.
     * @param resultSet the ResultSet to render
     * @return the CSV String
     * @throws SQLException
     */
    private String resultSetToString(final ResultSet resultSet) throws SQLException {
        return resultSet.getLong("id")
                + "," + resultSet.getLong("domain_id")
                + "," + resultSet.getLong("url_id")
                + "," + resultSet.getLong("keyword_id")
                + "," + resultSet.getInt("google_search_rank")
                + "," + resultSet.getInt("google_page_rank")
                + "," + resultSet.getInt("bing_search_rank");
    }

    @Override
    public void verify(final VerificationContext context) {
        /* Get the clients for S3 and the database. */

        final AmazonS3Client s3Client = getS3Client();
        final JdbcTemplate jdbcTemplate = getJdbcTemplate();

        /* Create a reader for the S3 data. */

        final S3ObjectInputStream strm = s3Client.getObject("searchlight-reports", "serp_items_report")
                .getObjectContent();
        final BufferedReader s3Reader = new BufferedReader(new InputStreamReader(strm));

        /* Execute a simple query against the legacy reporting database. The {@link ResultSetExtractor} is used to
        process the entire {@link ResultSet}. Note that there are other ways of stepping through a ResultSet using
        JdbcTemplate, such as using a {@link RowMapper}.
         */

        jdbcTemplate.query("SELECT * FROM keyword_url_facts", new ResultSetExtractor() {

            @Override
            public Void extractData(final ResultSet resultSet) throws SQLException {
                try {
                    while (true) {
                        /* Attempt to get the next line of data from both the S3 report and the database. */

                        final String s3Line = s3Reader.readLine();
                        final String mySqlLine = resultSet.next() ? resultSetToString(resultSet) : null;

                        /* If both sources of input have been drained, break out of the loop. */

                        if (s3Line == null && mySqlLine == null) {
                            break;
                        }

                        /* Otherwise, print the rows next to each other. */

                        System.out.println("S3: " + s3Line);
                        System.out.println("MySQL: " + mySqlLine);
                        System.out.println();
                    }
                } catch (final IOException ioe) {
                    System.out.println(ioe);
                }

                return null;
            }
        });
    }
}

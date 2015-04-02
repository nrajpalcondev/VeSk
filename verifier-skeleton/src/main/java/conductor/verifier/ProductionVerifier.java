package conductor.verifier;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;
import sun.awt.Mutex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * This {@link Verifier} implementation should actually check that all the rows in the database's keyword_url_facts
 * table were successfully migrated to S3, according to the rules laid out in the README.md file. See
 * {@link PrintingVerifier} for some clues on how to interact with the S3 report and the database.
 *
 * YOUR IMPLEMENTATION GOES BELOW...
 */
@Component("productionVerifier")
public class ProductionVerifier extends AbstractVerifier {

    final private LinkedList<String> s3Excess; /* excess s3 data */
    final private LinkedList<String> mySqlExcess; /* excess sql data */
    final private TreeMap<String, String> matches; /* in case this data becomes important later */

    Thread s3Thread; /* thread dealing with s3 functions */
    Thread sqlThread; /* thread dealing with sql functions */
    final Mutex mutex; /* protects memory access to the queues */

    /**
     * Complex default constructor used to initialize several variables
     */
    public ProductionVerifier() {
        s3Excess = new LinkedList<String>(); /* initialization of s3 queue */
        mySqlExcess = new LinkedList<String>(); /* initialization of sql queue */

        /* inline compare class to sort the TreeMap */
        class string_num_compare implements Comparator<String> {

            public int compare(String s1, String s2) {
                /* compare by id of each sql response */
                return Integer.parseInt(s1.split(",")[0]) - Integer.parseInt(s2.split(",")[0]);
            }
        };

        matches = new TreeMap<String, String>(new string_num_compare()); /* initialization of the TreeMap to sort data */
        mutex = new Mutex(); /*initialization of the mutex */
    }

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
                + "," + resultSet.getString("domain_name")
                + "," + resultSet.getString("url");
    }

    /**
     * Function used to retrieve s3 data
     * @param s3Reader
     * @return String: data from the s3 'server'
     * @throws IOException
     */
    private String extract(final BufferedReader s3Reader) throws IOException {

        /* return value */
        String ret = null;

        /* get s3 data */
        final String s3Line = s3Reader.readLine();

        if (s3Line != null) { /* check if null */
            ret = s3Line; /* set return to be the string */
        }

        return ret;
    }

    /**
     * Function used to retrieve sql data
     * @param resultSet
     * @return String: sql data from the 'server'
     * @throws SQLException
     */
    private String extract(final ResultSet resultSet) throws SQLException {

        /* return value */
        String ret = null;

        /* get mySql data */
        final String mySqlLine = resultSet.next() ? resultSetToString(resultSet) : null;

        if (mySqlLine != null) { /* check if null */
            ret = mySqlLine; /* set return to be the string */
        }

        return ret;
    }

    /**
     * Function used to compare sql and s3 strings
     * @param s3
     * @param sql
     * @return boolean: if they relate to the same query
     */
    private boolean compare(final String s3, final String sql) {

        boolean ret = false;

        if (s3 != null && sql != null) { /* avoid pesky NullPointerException */
            final String[] s3Split = s3.split(","); /* split string into parts */
            final String[] sqlSplit = sql.split(","); /* split string into parts */

            if (s3Split[0].equals(sqlSplit[3]) && /* keyword id */
                s3Split[4].equals(sqlSplit[4]) && /* s3 google classic rank vs sql google search rank */
                s3Split[5].equals(sqlSplit[5]) && /* s3 google true rank vs google page rank */
                s3Split[3].equals(sqlSplit[6]) && /* domain names */
                s3Split[2].equals(sqlSplit[7])) { /* urls */
                ret = true;
            }

            /* debug */
            //System.out.println(s3 + "\r\n" + sql + "\r\n");
        }

        return ret;
    }

    /**
     * Function to detect non standard links
     * @param s3
     * @return boolean: if link is standard
     */
    private boolean standard_link(final String s3) {
        boolean ret = false;

        if (s3 != null) { /* avoid pesky NullPointerException */
            final String[] s3Split = s3.split(",");

            /* check if STANDARD_LINK */
            if (s3Split[1].equals("STANDARD_LINK")) {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * Function to detect a null rank from google
     * @param sql
     * @return boolean: if rank is null
     */
    private boolean null_rank(final String sql) {
        boolean ret = false;

        if (sql != null) { /* avoid pesky NullPointerException */
            final String[] sqlSplit = sql.split(",");

            /* check if ranks are null */
            if (sqlSplit[4].equals("0") && sqlSplit[5].equals("0")) {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * Function used to add s3 data to it's desirable queue
     * @param data
     */
    private void add_s3(final String data) {

        if (standard_link(data)) { /* if standard link */
            for (int i = 0; i < mySqlExcess.size(); i++) { /* for each excess sql entry */

                if (compare(data, mySqlExcess.get(i))) { /* if they compare, add to matches */
                    matches.put(mySqlExcess.get(i), data);
                    mySqlExcess.remove(i); /* remove redundancy */

                /* debug *//*
                    System.out.println("New match!");
                    System.out.println("s3: " + matches.lastEntry().getValue());
                    System.out.println("MySQL: " + matches.lastEntry().getKey());
                    System.out.println();*/
                    return;
                }
            }

            /* if queue has been looked through and value not null */
            if (data != null) {
                s3Excess.add(data); /* no matches */

                /* debug *//*
                System.out.println("s3 data added to s3Excess");
                System.out.println("s3: " + data);
                System.out.println();*/
            }
        }
    }

    /**
     * Function used to add sql data to it's desirable queue
     * @param data
     */
    private void add_sql(final String data) {

        if (!null_rank(data)) { /* if the rankings are not null */
            for (int i = 0; i < s3Excess.size(); i++) { /* for each excess s3 entry */

                if (compare(s3Excess.get(i), data)) { /* if they compare, add to matches */
                    matches.put(data, s3Excess.get(i));
                    s3Excess.remove(i); /* remove redundancy */

                    /* debug *//*
                    System.out.println("New match!");
                    System.out.println("s3: " + matches.lastEntry().getValue());
                    System.out.println("MySQL: " + matches.lastEntry().getKey());
                    System.out.println();*/
                    return;
                }
            }

            /* if queue has been looked through and value not null */
            if (data != null) {
                mySqlExcess.add(data); /* no matches */

                /* debug *//*
                System.out.println("sql data added to mySqlExcess");
                System.out.println("sql: " + data);
                System.out.println();*/
            }
        }
    }

    @Override
    public void verify(final VerificationContext context) {
        /* Get the clients for S3 and the database. */

        final AmazonS3Client s3Client = getS3Client();
        final JdbcTemplate jdbcTemplate = getJdbcTemplate();

        /* Create a reader for the S3 data. */

        final S3ObjectInputStream strm = s3Client.getObject("searchlight-reports", "serp_items_report")
                .getObjectContent(); /* When placed in a constructor, this throws a NullPointer */
        final BufferedReader s3Reader = new BufferedReader(new InputStreamReader(strm));

        /* Execute a simple query against the legacy reporting database. The {@link ResultSetExtractor} is used to
        process the entire {@link ResultSet}. Note that there are other ways of stepping through a ResultSet using
        JdbcTemplate, such as using a {@link RowMapper}.
         */

        /* sql query to find id, domain id, url id, keword id, google search rank, and google page rank of
           keyword_url_facts, domain name of domains, and url of urls
         */
        final String sql_query = "SELECT a.id, a.domain_id, a.url_id, a.keyword_id, " +
                                 "a.google_search_rank, a.google_page_rank, " +
                                 "b.domain_name, d.url " +
                                 "FROM keyword_url_facts a, domains b, urls d " +
                                 "WHERE b.id = a.domain_id AND d.id = a.url_id";

        jdbcTemplate.query(sql_query, new ResultSetExtractor() { /* sql call */

            public Void extractData(final ResultSet resultSet) throws SQLException {

                s3Thread = new Thread() { /* declare thread to run s3 functions */
                    public void run() {
                        try {
                            String s3; /* s3 data */

                            do {
                                s3 = extract(s3Reader); /* get s3 data */

                                mutex.lock(); /* waits for the queues */
                                add_s3(s3); /* add s3 data to a queue */
                                mutex.unlock(); /* lets others use the queues */
                            } while (s3 != null); /* while there's still data */
                        } catch (final IOException e) {
                            System.err.println(e.getStackTrace()[0].getLineNumber() + ": " + e);
                        }
                    }
                };

                sqlThread = new Thread() { /* declare thread to run sql functions */
                    public void run() {
                        try {
                            String sql; /* sql data */

                            do {
                                sql = extract(resultSet); /* get s3 data */

                                mutex.lock(); /* waits for the queues */
                                add_sql(sql); /* add s3 data to a queue */
                                /*Thread.sleep(1000); // debug
                                System.out.println(sql);*/
                                mutex.unlock(); /* lets others use the queues */
                            } while (sql != null); /* while there's still data */
                        } catch (final SQLException e) {
                            System.err.println(e.getStackTrace()[0].getLineNumber() + ": " + e);
                        } /*catch (final InterruptedException e) { // debug
                            System.err.println(e.getStackTrace()[0].getLineNumber() + ": " + e);
                        }*/
                    }
                };

                s3Thread.start(); /* start s3 thread */
                sqlThread.start(); /* start sql thread */

                try {
                    s3Thread.join(); /* wait for s3 thread to end */
                    sqlThread.join(); /* wait for sql thread to end */
                } catch (InterruptedException e) {
                    System.err.println(e.getStackTrace()[0].getLineNumber() + ": " + e);
                }

                /* matched sql and s3 rows output */
                System.out.println("Matched S3 and SQL Rows:");
                Iterator mi = matches.entrySet().iterator();
                while (mi.hasNext()) {
                    Map.Entry me = (Map.Entry)mi.next();
                    System.out.println("s3: " + me.getValue());
                    System.out.println("sql: " + me.getKey());
                    System.out.println();
                }
                System.out.println();

                /* s3 output */
                System.out.println("Corrupt S3 Rows:");
                for (String ss3 : s3Excess) {
                    System.out.println(ss3);
                    context.corruptRow(ss3);
                }
                System.out.println();

                /* sql output */
                System.out.println("Missing SQL Rows:");
                for (String ssql : mySqlExcess) {
                    System.out.println(ssql);
                    context.missingRow(ssql);
                }
                System.out.println();

                return null;
            }
        });
    }
}

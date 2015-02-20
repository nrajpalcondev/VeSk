package conductor.verifier;

/**
 * Provides a mechanism for recording the outcome of the verification process with respect to corrupt and missing rows.
 */
public interface VerificationContext {

    /**
     * Record a row of data from the MySQL database that should have been migrated to S3 but wasn't.
     *
     * @param sqlRow A string representation of the missing row
     */
    public void missingRow(String sqlRow);

    /**
     * Record a row of data in the S3 report that was corrupted during the migration or otherwise fails to match a row
     * from the MySQL database
     *
     * @param row the corrupt line from the S3 report
     */
    public void corruptRow(String row);
}

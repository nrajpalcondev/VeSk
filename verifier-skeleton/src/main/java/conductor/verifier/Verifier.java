package conductor.verifier;

/**
 * A Verifier is responsible for identifying corrupt or un-migrated data between our legacy reporting database and our
 * new S3 reporting warehouse.
 */
public interface Verifier {

    /**
     * Verify that a successful migration has occurred from the database to S3.
     *
     * @param context the VerificationContext to use to record problematic rows
     */
    public void verify(VerificationContext context);
}

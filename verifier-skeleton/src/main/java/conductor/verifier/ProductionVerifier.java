package conductor.verifier;

import org.springframework.stereotype.Component;

/**
 * This {@link Verifier} implementation should actually check that all the rows in the database's keyword_url_facts
 * table were successfully migrated to S3, according to the rules laid out in the README.md file. See
 * {@link PrintingVerifier} for some clues on how to interact with the S3 report and the database.
 *
 * YOUR IMPLEMENTATION GOES BELOW...
 */
@Component("productionVerifier")
public class ProductionVerifier extends AbstractVerifier {

    @Override
    public void verify(final VerificationContext context) {
        // TODO: Your implementation goes here!

    }
}

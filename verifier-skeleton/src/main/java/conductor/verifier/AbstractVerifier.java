package conductor.verifier;

import com.amazonaws.services.s3.AmazonS3Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class AbstractVerifier implements Verifier {

    @Autowired
    @Qualifier("embeddedJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Autowired
    @Qualifier("mockAmazonS3Client")
    private AmazonS3Client amazonS3Client;

    protected JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    protected AmazonS3Client getS3Client() {
        return amazonS3Client;
    }
}

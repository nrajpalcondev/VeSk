package conductor.verifier.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class creates a mock version of the Amazon S3 API and fills it up with fixture data. Don't modify this class
 * unless you know what you're doing!
 */
@Component
public class AmazonS3ClientFactory {

    private S3Object createObject() throws IOException {
        final S3Object obj = new S3Object();

        obj.setBucketName("searchlight-reports");
        obj.setKey("serp_items_report");
        obj.setObjectContent(new ClassPathResource("serp_items_report.csv").getInputStream());
        obj.setObjectMetadata(null);

        return obj;
    }

    @Bean(name = "mockAmazonS3Client")
    @Scope("prototype")
    public AmazonS3Client createAmazonS3Client() {
        final AmazonS3Client amazonS3Client = mock(AmazonS3Client.class);

        when(amazonS3Client.getObject(any(GetObjectRequest.class))).thenAnswer(new Answer<S3Object>() {
            @Override
            public S3Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                final GetObjectRequest request = (GetObjectRequest) invocationOnMock.getArguments()[0];

                if (request.getBucketName().equals("searchlight-reports")
                        && request.getKey().equals("serp_items_report")) {
                    return createObject();
                } else {
                    return null;
                }
            }
        });
        when(amazonS3Client.getObject("searchlight-reports", "serp_items_report")).thenAnswer(new Answer<S3Object>() {

            @Override
            public S3Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return createObject();
            }
        });
        when(amazonS3Client.getObject(any(GetObjectRequest.class), any(File.class)))
                .thenThrow(new UnsupportedOperationException());

        return amazonS3Client;
    }
}

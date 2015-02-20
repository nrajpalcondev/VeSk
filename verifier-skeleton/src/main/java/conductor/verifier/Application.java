package conductor.verifier;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * This class is the entry point to the verification system.
 */
@Configuration
@ComponentScan
public class Application {

    public static void main(final String[] args) {
        /* Initialize the Spring application context. */
        ApplicationContext context = new AnnotationConfigApplicationContext(Application.class);

        /* Grab a handle to the no-op printing verifier. Change this to "productionVerifier" once your implementation
         is working... */
        final Verifier verifier = (Verifier) context.getBean("printingVerifier");

        /* Create a VerificationContext to capture the output of verification. */
        final BasicVerificationContext verificationContext = new BasicVerificationContext();

        /* Run it! */
        verifier.verify(verificationContext);

        System.out.println(verificationContext);
    }
}

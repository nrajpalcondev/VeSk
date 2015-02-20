package conductor.verifier.db;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

/**
 * This class creates a Spring {@link JdbcTemplate} backed by an in-memory database full of fixture data. Don't modify
 * this code unless you really know what you're doing!
 */
@Component
public class JdbcTemplateFactory {

    @Bean(name = "embeddedJdbcTemplate")
    public JdbcTemplate createJdbcTemplate() throws SQLException {
        final EmbeddedDatabase db = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.HSQL)
                .addScript("domains.sql")
                .addScript("keywords.sql")
                .addScript("urls.sql")
                .addScript("keyword_url_facts.sql").build();

        return new JdbcTemplate(db);
    }
}

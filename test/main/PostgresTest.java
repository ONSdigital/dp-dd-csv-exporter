package main;

import org.apache.commons.io.FileUtils;
import play.Logger;
import javax.persistence.*;
import java.io.File;
import java.io.IOException;
import static org.testng.Assert.fail;


public class PostgresTest {

    private static Logger.ALogger logger = Logger.of(PostgresTest.class);

    public void createDatabase(EntityManager em) throws Exception {
        // this lot will insert the data that was create by the Open-Dta-small.csv file in the db-loader project
        runDbScript(em, "sql/data_discovery_public_taxonomy.sql");
        runDbScript(em, "sql/data_discovery_public_data_resource.sql");
        runDbScript(em, "sql/data_discovery_public_data_resource_taxonomy.sql");
        runDbScript(em, "sql/data_discovery_public_geographic_area_hierarchy.sql");
        runDbScript(em, "sql/data_discovery_public_geographic_area_type.sql");
        runDbScript(em, "sql/data_discovery_public_geographic_level_type.sql");
        runDbScript(em, "sql/data_discovery_public_geographic_area.sql");
        runDbScript(em, "sql/data_discovery_public_time_type.sql");
        runDbScript(em, "sql/data_discovery_public_time_period.sql");
        runDbScript(em, "sql/data_discovery_public_unit_type.sql");
        runDbScript(em, "sql/data_discovery_public_population.sql");
        runDbScript(em, "sql/data_discovery_public_value_domain.sql");
        runDbScript(em, "sql/data_discovery_public_subject_field.sql");
        runDbScript(em, "sql/data_discovery_public_concept_system.sql");
        runDbScript(em, "sql/data_discovery_public_subject_field_concept_system.sql");
        runDbScript(em, "sql/data_discovery_public_category.sql");
        runDbScript(em, "sql/data_discovery_public_variable.sql");
        runDbScript(em, "sql/data_discovery_public_variable_category.sql");
        runDbScript(em, "sql/data_discovery_public_dimensional_data_set.sql");
        runDbScript(em, "sql/data_discovery_public_dimensional_data_point.sql");
        runDbScript(em, "sql/data_discovery_public_observation_type.sql");
        runDbScript(em, "sql/data_discovery_public_presentation_type.sql");
        runDbScript(em, "sql/data_discovery_public_presentation.sql");

        // todo remove the stage tables altogether once the code writes directly
        runDbScript(em, "sql/data_discovery_public_stage_dimensional_data_point.sql");
        runDbScript(em, "sql/data_discovery_public_stage_category.sql");
    }

    private void runDbScript(EntityManager em, String filename) throws IOException {
        File inputFile = new File(PostgresTest.class.getResource(filename).getPath());
        String sqlScript = FileUtils.readFileToString(inputFile, "UTF-8");
        Query query = em.createNativeQuery(sqlScript);

        if(! sqlScript.isEmpty()) {
            try {
                EntityTransaction tx = em.getTransaction();
                tx.begin();
                query.executeUpdate();
                tx.commit();
            } catch (Exception e) {
                e.printStackTrace();
                fail();
            }
        } else {
            logger.debug("Skipping empty sql script for " + filename);
        }
    }

}

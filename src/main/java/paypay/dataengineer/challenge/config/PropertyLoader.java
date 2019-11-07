package paypay.dataengineer.challenge.config;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paypay.dataengineer.challenge.utils.UtilityMethods;

import java.io.Serializable;
import java.util.Properties;

@Data
public class PropertyLoader implements Serializable {
    private static Logger LOG = LoggerFactory.getLogger(PropertyLoader.class);
    private static PropertyLoader sparkPropertyLoader;
    private String masterUrl;
    private String sparkDriver;
    private String awsRegion;
    private String s3Bucket;
    private String environment;
    private Integer streamingInterval;

    public PropertyLoader(String environment) {
        this.environment = environment;
        setVariables(UtilityMethods.loadPropertiesFromResource(environment + ".properties"));
    }

    private void setVariables(Properties properties) {
        setMasterUrl(properties.getProperty("master.url", "local"));
        setSparkDriver(properties.getProperty("spark.driver.allowMultipleContexts"));
        setAwsRegion(properties.getProperty("fs.s3.bucket.region"));
        setS3Bucket(properties.getProperty("s3.bucket.name"));
        setStreamingInterval(Integer.parseInt(properties.getProperty("streaming.interval", "1000")));
        LOG.info("Properties: {}", properties);
    }

    public static PropertyLoader getInstance(String environment) {
        if (sparkPropertyLoader == null) {
            synchronized (PropertyLoader.class) {
                if (sparkPropertyLoader == null) {
                    sparkPropertyLoader = new PropertyLoader(environment);
                }
                return sparkPropertyLoader;
            }
        }
        return sparkPropertyLoader;
    }
}


package paypay.dataengineer.challenge.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UtilityMethods {

    private final static Pattern PATTERN = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"(\\S+ \\S+ \\S+)\" \"([^\"]*)\" (\\S+) (\\S+)");

    public static Row parseLine(String lineText) {
        Matcher matcher=PATTERN.matcher(lineText);
        matcher.find();
        String[] backendIpPort = {"-", "-"};
        String[] clientIpPort = {"-", "-"};
        if (!matcher.group(4).equalsIgnoreCase("-"))
            backendIpPort = matcher.group(4).split(":");
        if (!matcher.group(3).equalsIgnoreCase("-"))
            clientIpPort = matcher.group(3).split(":");
        return RowFactory.create(matcher.group(1),
                matcher.group(2),
                clientIpPort[0],
                clientIpPort[1],
                backendIpPort[0],
                backendIpPort[1],
                matcher.group(5),
                matcher.group(6),
                matcher.group(7),
                matcher.group(8),
                matcher.group(9),
                matcher.group(10),
                matcher.group(11),
                matcher.group(12),
                matcher.group(13),
                matcher.group(14),
                matcher.group(15)) ;
    }

    public static StructField createStructField(String name, DataType dataType){
        return DataTypes.createStructField(name, dataType, Boolean.TRUE);
    }

    public static StructType createAndGetSchema() {
        return DataTypes.createStructType(
                Arrays.asList(
                        createStructField("timeStamp", DataTypes.StringType),
                        createStructField("loadBalancerName", DataTypes.StringType),
                        createStructField("clientIp", DataTypes.StringType),
                        createStructField("clientPort", DataTypes.StringType),
                        createStructField("backendIp", DataTypes.StringType),
                        createStructField("backendPort", DataTypes.StringType),
                        createStructField("requestProcessingTimeInSeconds", DataTypes.StringType),
                        createStructField("backendProcessingTimeInSeconds", DataTypes.StringType),
                        createStructField("responseProcessingTime", DataTypes.StringType),
                        createStructField("elbStatusCode", DataTypes.StringType),
                        createStructField("backendStatusCode", DataTypes.StringType),
                        createStructField("receivedBytes", DataTypes.StringType),
                        createStructField("sentBytes", DataTypes.StringType),
                        createStructField("request", DataTypes.StringType),
                        createStructField("userAgent", DataTypes.StringType),
                        createStructField("sslCipher", DataTypes.StringType),
                        createStructField("sslProtocol", DataTypes.StringType)
                )
        );
    }

    public static Seq<String> getScalaSeqColumns(List<String> javaColumnArray) {
        return JavaConverters.asScalaIteratorConverter(javaColumnArray.iterator())
                .asScala()
                .toSeq();
    }

    /**
     * Simple util function to check for desired no of args
     * @param args
     * @param expectedLength
     * @return
     */
    public static boolean checkArgs(String[] args, int expectedLength) {
        return !(args.length >= expectedLength);
    }

    /**
     * Function to load desried env properties from resources
     * @param filePath
     * @return
     */
    public static Properties loadPropertiesFromResource(String filePath) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = UtilityMethods.class.getClassLoader().getResourceAsStream(filePath);
            properties.load(inputStream);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        if (properties.size() < 1) {
            System.exit(Constants.ExitCodes.PLATFORM_PROPERTIES_NOT_LOADED.getExitCode());
            return null;
        }
        return properties;
    }
}

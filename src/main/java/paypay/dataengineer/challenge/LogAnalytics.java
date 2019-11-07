package paypay.dataengineer.challenge;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import paypay.dataengineer.challenge.utils.Constants.ExitCodes;
import paypay.dataengineer.challenge.utils.UtilityMethods;

import java.util.ArrayList;
import java.util.List;

public class LogAnalytics extends SparkApplication {

    private String logPath = "data/input_log_file.log";
    private String env;

    public LogAnalytics(String env, String logPath) {
        super();
        this.env = env;
        this.logPath = logPath;
    }

    private Dataset loadFromPath(String logFilePath) {
        Dataset<String> dataset = sparkSession.read().textFile(logFilePath);
        Dataset parsedDataset = dataset.map((MapFunction<String, Row>) UtilityMethods::parseLine,
                RowEncoder.apply(UtilityMethods.createAndGetSchema()));
        parsedDataset = parsedDataset.withColumn("timeStamp", parsedDataset.col("timeStamp").cast(DataTypes.TimestampType));
        return parsedDataset;
    }

    private Dataset sessionize(Dataset mainDataset) {
        Dataset sessionDataset = mainDataset.select(
                functions.window(mainDataset.col("timeStamp"),
                        ("15 minutes"))
                        .as("fixedSessionWindow"),
                mainDataset.col("timeStamp"),
                mainDataset.col("clientIp"))
                .groupBy("fixedSessionWindow",
                        "clientIp")
                .count()
                .withColumnRenamed("count",
                        "numberHitsInSessionForIp");
        sessionDataset = sessionDataset.withColumn("sessionId",
                functions.monotonicallyIncreasingId());
        return sessionDataset;
    }

    private Dataset determineUniqueURLVisitPerSession(Dataset sessionizeDataset) {
        return sessionizeDataset.groupBy("sessionId",
                "request")
                .count()
                .distinct()
                .withColumnRenamed("count", "urlHitCount");
    }

    private Dataset mostEngagedUsers(Dataset sessionizeWithDurationDataset) {
        return sessionizeWithDurationDataset.select("clientIp",
                "sessionId",
                "sessionDuration")
                .sort(sessionizeWithDurationDataset.col("sessionDuration").desc())
                .distinct();
    }

    private Dataset calculateAvergaeSessionDurations(Dataset sessionizeWithDurationDataset) {
        return sessionizeWithDurationDataset.select(functions.avg("sessionDuration")
                .alias("averageSessionDuration"));
    }

    private Dataset calculateSessionDurations(Dataset mainDataset, Dataset sessionizeDataset) {
        Dataset datasetWithTimeStamps = mainDataset.select(functions.window(mainDataset.col("timeStamp"), "15 minutes").alias("fixedSessionWindow"), mainDataset.col("timeStamp"), mainDataset.col("clientIp"), mainDataset.col("request"));
        List<String> columsToJoinAt = new ArrayList<>();
        columsToJoinAt.add("fixedSessionWindow");
        columsToJoinAt.add("clientIp");
        Dataset sessionizeWithDurationDataset = datasetWithTimeStamps.join(sessionizeDataset, UtilityMethods.getScalaSeqColumns(columsToJoinAt));
        Dataset firstHitTimeStamps = sessionizeWithDurationDataset.groupBy("sessionId").agg(functions.min("timeStamp").alias("firstHitTimeStamp"));
        sessionizeWithDurationDataset = firstHitTimeStamps.join(sessionizeWithDurationDataset, "sessionId");
        sessionizeWithDurationDataset = sessionizeWithDurationDataset.withColumn("timeDiffwithFirstHit",
                functions.unix_timestamp(sessionizeWithDurationDataset.col("timeStamp"))
                        .minus(functions.unix_timestamp(sessionizeWithDurationDataset.col("firstHitTimeStamp"))));
        Dataset tmpdf = sessionizeWithDurationDataset.groupBy("sessionId").agg(functions.max("timeDiffwithFirstHit").alias("sessionDuration"));
        sessionizeWithDurationDataset = sessionizeWithDurationDataset.join(tmpdf, "sessionId");
        return sessionizeWithDurationDataset;
    }

    @Override
    public void run(String[] args) throws Exception {
        Dataset mainDataset = loadFromPath(logPath);
        Dataset sessionizeDataset = sessionize(mainDataset);
        mainDataset.cache();
        sessionizeDataset.cache();
        Dataset sessionizeWithDurationDataset = calculateSessionDurations(mainDataset, sessionizeDataset);
        sessionizeWithDurationDataset.cache();
        Dataset averageSessionDurationsDataset = calculateAvergaeSessionDurations(sessionizeWithDurationDataset);
        Dataset uniqueVisitDataset = determineUniqueURLVisitPerSession(sessionizeWithDurationDataset);
        Dataset mostEngaged = mostEngagedUsers(sessionizeWithDurationDataset);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage env logPath");
            System.exit(ExitCodes.INSUFFICIENT_ARGS.getExitCode());
        }

        String env = args[0];
        String logPath = args[1];

        SparkApplication logAnalytics = new LogAnalytics(env, logPath);
        logAnalytics.init(args)
                .setSparkConf()
                .setJavaSparkContext()
                .run(args);
    }
}

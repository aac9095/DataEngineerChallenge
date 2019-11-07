package paypay.dataengineer.challenge;

import lombok.Getter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import paypay.dataengineer.challenge.config.PropertyLoader;
import paypay.dataengineer.challenge.utils.Constants;
import paypay.dataengineer.challenge.utils.LoggerUtils;
import paypay.dataengineer.challenge.utils.UtilityMethods;

/**
 * Abstract class for Spark jobs
 *
 * @author Ayush Chauhan
 */
@Getter
public abstract class SparkApplication {
    public String environment;
    public SparkSession sparkSession;
    public SparkConf sparkConf;
    public JavaSparkContext javaSparkContext;
    public PropertyLoader sparkPropertyLoader;
    public Logger logger;

    /**
     * This will initialize the property loader and logger for the child classes
     * @param args parameterised args with env being the first argument
     * @return the current object
     */
    public SparkApplication init(String[] args) {
        this.logger = LoggerUtils.getInstance();
        if (UtilityMethods.checkArgs(args, 1)) {
            logger.info("args check failed. exiting");
            System.exit(Constants.ExitCodes.INSUFFICIENT_ARGS.getExitCode());

        }

        this.environment = args[0];
        this.sparkPropertyLoader = PropertyLoader.getInstance(environment);
        return this;
    }

    /**
     * This will return the SparkConf object initialized with basic properties
     * @return SparkConf object
     */
    private SparkConf getBasicSparkConf() {
        return new SparkConf()
                .set("fs.s3.buckets.create.region", sparkPropertyLoader.getAwsRegion())
                .set("spark.driver.allowMultipleContexts", sparkPropertyLoader.getSparkDriver())
                .set("spark.streaming.driver.writeAheadLog.allowBatching", "false")
                .set("spark.executor.logs.rolling.strategy", "time")
                .set("spark.executor.logs.rolling.time.interval", "daily")
                .set("spark.executor.logs.rolling.enableCompression", "true")
                .set("spark.executor.logs.rolling.maxRetainedFiles", "3")
                .set("spark.kryoserializer.buffer.max", "1g")
                .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .setAppName(this.getClass().getName());
    }

    /**
     * This will initialize the sparkConf with the basic properties
     * @return the current object
     */
    public SparkApplication setSparkConf() {
        if(this.sparkConf == null){
            this.sparkConf = getBasicSparkConf();
        }
        return this;
    }

    /**
     * This will initialize the sparkConf with basic properties if not already done
     * and set the passed property if it isn't already configured
     * @param propName name of the spark conf property
     * @param propVal value of the spark conf property
     * @return the current object
     */
    public SparkApplication setSparkConf(String propName, String propVal){
        if(this.sparkConf == null){
            this.sparkConf = getBasicSparkConf();
        }

        this.sparkConf.set(propName, propVal);
        return this;
    }

    /**
     * This will initialize the spark session and spark context
     * @return the current object
     */
    public SparkApplication setJavaSparkContext(){
        assert (this.sparkConf != null);
        this.sparkSession = SparkSession.builder().master(sparkPropertyLoader.getMasterUrl()).config(sparkConf)
                .enableHiveSupport().getOrCreate();
        this.javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        return this;
    }

    /**
     * Run the instance of the SparkApplication.
     * @param args parameterised args with env being the first argument
     * @throws Exception
     */
    public abstract void run(String[] args) throws Exception;

}


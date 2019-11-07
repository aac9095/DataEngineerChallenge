package paypay.dataengineer.challenge.utils;

import org.apache.log4j.Logger;

public class LoggerUtils {
    private static Logger logger;

    /**
     * private constructor to avoid client applications to use constructor
     */
    private LoggerUtils() {

    }

    public static Logger getInstance(){
        if(logger == null){
            logger = Logger.getLogger(LoggerUtils.class);
        }
        return logger;
    }

    public static void info(Object info) {
        LoggerUtils.getInstance().info(info);
    }
    public static void error(Object info) {
        LoggerUtils.getInstance().error(info);
    }
}

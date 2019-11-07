package paypay.dataengineer.challenge.utils;

public class Constants {
    public static final String ENVIRONMENT = "environment";

    public static final long SECOND = 1000;
    public static final long MINUTE = 60 * SECOND;
    public static final long HOUR = 60 * MINUTE;
    public static final long DAY = 24 * HOUR;


    public enum ExitCodes {
        INSUFFICIENT_ARGS(1),
        EXCEPTION(2),
        PLATFORM_PROPERTIES_NOT_LOADED(5);

        private int exitCode;

        public int getExitCode() {
            return exitCode;
        }

        ExitCodes(int exitCode) {
            this.exitCode = exitCode;
        }
    }
}

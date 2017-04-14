package spark.applications.util;

/**
 * Created by I309939 on 5/3/2016.
 */
public final class OsUtils {
    private static String OS = System.getProperty("os.name").toLowerCase();


    public static void detectOS() {
        if (isWindows()) {

        } else if (isMac()) {

        } else if (isUnix()) {

        } else {

        }
    }

    public static String OS_wrapper(String path) {
        if (isWindows()) {
            return "\\" + path;
        } else if (isMac()) {
            return "/" + path;
        } else if (isUnix()) {
            return "/" + path;
        } else {
            return null;
        }
    }

    public static boolean isWindows() {
        return (OS.indexOf("win") >= 0);
    }

    public static boolean isMac() {
        return (OS.indexOf("mac") >= 0);
    }

    public static boolean isUnix() {
        return (OS.indexOf("nux") >= 0);
    }
}
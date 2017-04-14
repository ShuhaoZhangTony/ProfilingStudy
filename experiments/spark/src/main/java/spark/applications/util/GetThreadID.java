package spark.applications.util;

public class GetThreadID {
    static {
        System.loadLibrary("GetThreadID");
    }

    public static native int get_tid();
}
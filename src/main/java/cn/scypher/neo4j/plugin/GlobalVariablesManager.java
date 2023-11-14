package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;

public class GlobalVariablesManager {

    private static String timePointType = "localdatetime";

    private static String timezone = null;

    private static STimePoint snapshotTimePoint = null;
    private static SInterval scopeInterval = null;

    public static String getTimePointType() {
        return timePointType;
    }

    public static void setTimePointType(String timePointType) {
        GlobalVariablesManager.timePointType = timePointType;
    }

    public static String getTimezone() {
        return timezone;
    }

    public static void setTimezone(String timezone) {
        GlobalVariablesManager.timezone = timezone;
    }

    public static STimePoint getSnapshotTimePoint() {
        return snapshotTimePoint;
    }

    public static void setSnapshotTimePoint(STimePoint snapshotTimePoint) {
        GlobalVariablesManager.snapshotTimePoint = snapshotTimePoint;
    }

    public static SInterval getScopeInterval() {
        return scopeInterval;
    }

    public static void setScopeInterval(SInterval scopeInterval) {
        GlobalVariablesManager.scopeInterval = scopeInterval;
    }
}

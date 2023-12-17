package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;

public class GlobalVariablesManager {

    private static String timePointType = "datetime";

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
        if (snapshotTimePoint == null | (snapshotTimePoint != null && snapshotTimePoint.getTimePointType().equals(GlobalVariablesManager.timePointType))) {
            GlobalVariablesManager.snapshotTimePoint = snapshotTimePoint;
        } else {
            throw new RuntimeException("The time point type of snapshot must be consistent with the system");
        }
    }

    public static SInterval getScopeInterval() {
        return scopeInterval;
    }

    public static void setScopeInterval(SInterval scopeInterval) {
        if (scopeInterval == null | (scopeInterval != null && scopeInterval.getTimePointType().equals(GlobalVariablesManager.timePointType))) {
            GlobalVariablesManager.scopeInterval = scopeInterval;
        } else {
            throw new RuntimeException("The interval type of scope must be consistent with the system");
        }
    }
}

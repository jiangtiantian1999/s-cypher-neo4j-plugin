package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SDateTimeOperation {

    @Context
    public Transaction tx;

    /**
     * @param timePoint 一个时间点，为时间点类型、str类型或Map类型
     * @return 返回当前时区和当前时间粒度下的时间点
     */
    @UserFunction("scypher.timePoint")
    @Description("Set a time point in system timezone and system time granularity.")
    public Object timePoint(@Name("timePoint") Object timePoint) {
        if (timePoint != null) {
            if (timePoint instanceof String | timePoint instanceof Map) {
                GlobalVariableManage globalVariableManage = new GlobalVariableManage(this.tx);
                String timeGranularity = globalVariableManage.getTimeGranularity();
                String timezone = globalVariableManage.getTimeZone();
                return (new STimePoint(timePoint, timeGranularity, timezone)).getSystemTimePoint();
            } else {
                // timePoint为时间点类型
                return (new STimePoint(timePoint)).getSystemTimePoint();
            }
        } else {
            throw new RuntimeException("Missing parameter timePoint.");
        }
    }

    /**
     * @param intervalFromObject 开始时间点，为时间点类型、str类型或Map类型
     * @param intervalToObject   结束时间点，为时间点类型、str类型或Map类型
     * @return 返回Map类型（具有两个key：from和to，value为时间点类型）
     */
    @UserFunction("scypher.interval")
    @Description("Set a interval.")
    public Map<String, Object> interval(@Name("intervalFrom") Object intervalFromObject, @Name("intervalTo") Object intervalToObject) {
        if (intervalFromObject != null && intervalToObject != null) {
            Map<String, Object> interval = new HashMap<>();
            GlobalVariableManage globalVariableManage = new GlobalVariableManage(this.tx);
            String timeGranularity = globalVariableManage.getTimeGranularity();
            String timezone = globalVariableManage.getTimeZone();
            STimePoint intervalFrom, intervalTo;
            if (intervalFromObject instanceof String | intervalFromObject instanceof Map) {
                intervalFrom = new STimePoint(intervalFromObject, timeGranularity, timezone);
            } else {
                // 开始时间为时间点类型
                intervalFrom = new STimePoint(intervalFromObject);
            }
            interval.put("from", intervalFrom.getSystemTimePoint());
            if (intervalToObject instanceof String | intervalToObject instanceof Map) {
                intervalTo = new STimePoint(intervalToObject, timeGranularity, timezone);
            } else {
                // 开始时间为时间点类型
                intervalTo = new STimePoint(intervalToObject);
            }
            interval.put("to", intervalTo.getSystemTimePoint());
            return interval;
        } else {
            throw new RuntimeException("Missing parameter intervalFrom or intervalTo.");
        }
    }

    /**
     * @param intervalMapList 时间区间的列表
     * @return 返回列表中所有时间区间的交集
     */
    @UserFunction("scypher.interval.intersection")
    @Description("Get the intersection of all intervals in a List.")
    public Map<String, Object> intervalIntersection(@Name("intervalList") List<Map<String, Object>> intervalMapList) {
        if (intervalMapList != null) {
            if (intervalMapList.size() > 0) {
                SInterval interval = new SInterval(intervalMapList.get(0));
                int index = 1;
                while (index < intervalMapList.size() && interval != null) {
                    interval = interval.intersection(new SInterval(intervalMapList.get(index++)));
                }
                if (interval != null) {
                    Map<String, Object> intervalMap = new HashMap<>();
                    intervalMap.put("from", interval.getIntervalFrom().getSystemTimePoint());
                    intervalMap.put("to", interval.getIntervalTo().getSystemTimePoint());
                    return intervalMap;
                } else {
                    return null;
                }
            } else {
                throw new RuntimeException("The intervalList can't be empty.");
            }
        } else {
            throw new RuntimeException("Missing parameter intervalList.");
        }
    }

    @UserFunction("scypher.interval.range")
    @Description("Get a interval within which all intervals in List are located.")
    public Map<String, Object> intervalRange(@Name("intervalList") List<Map<String, Object>> intervalMapList) {
        if (intervalMapList != null) {
            if (intervalMapList.size() > 0) {
                SInterval interval = new SInterval(intervalMapList.get(0));
                int index = 1;
                while (index < intervalMapList.size()) {
                    interval = interval.range(new SInterval(intervalMapList.get(index++)));
                }
                Map<String, Object> intervalMap = new HashMap<>();
                intervalMap.put("from", interval.getIntervalFrom().getSystemTimePoint());
                intervalMap.put("to", interval.getIntervalTo().getSystemTimePoint());
                return intervalMap;
            } else {
                throw new RuntimeException("The intervalList can't be empty.");
            }
        } else {
            throw new RuntimeException("Missing parameter intervalList.");
        }
    }

    /**
     * @param intervalMap1 一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @param intervalMap2 一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @return 返回两个时间区间的时间差
     */
    @UserFunction("scypher.interval.difference")
    @Description("Get the time difference between two intervals.")
    public TemporalAmount intervalDifference(@Name("interval1") Map<String, Object> intervalMap1, @Name("interval2") Map<String, Object> intervalMap2) {
        if (intervalMap1 != null && intervalMap2 != null) {
            SInterval interval1 = new SInterval(intervalMap1);
            SInterval interval2 = new SInterval(intervalMap2);
            return interval1.difference(interval2).getDuration();
        } else {
            throw new RuntimeException("Missing parameter interval1 or interval2.");
        }
    }


    /**
     * @param timeElement 一个时间点或时间区间，为时间点类型或Map类型（具有两个key：from和to，value为时间点类型）
     * @param intervalMap 一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @return 判定timeElement是否在intervalObject的区间内
     */
    @UserFunction("scypher.during")
    @Description("Do DURING operation between a time point/interval and a interval.")
    public boolean during(@Name("timeElement") Object timeElement, @Name("interval") Map<String, Object> intervalMap) {
        if (timeElement != null && intervalMap != null) {
            // 解析timeElement
            if (timeElement instanceof Map) {
                // timeElement为时间区间
                SInterval intervalLeft = new SInterval((Map<String, Object>) timeElement);
                SInterval intervalRight = new SInterval(intervalMap);
                return intervalRight.contains(intervalLeft);
            } else {
                // timeElement为时间点
                STimePoint timePoint = new STimePoint(timeElement);
                SInterval interval = new SInterval(intervalMap);
                return interval.contains(timePoint);
            }
        } else {
            throw new RuntimeException("Missing parameter timeElement(a time point or interval) or interval.");
        }
    }

    /**
     * @param interval1 一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @param interval2 一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @return 判定interval1和interval2是否有重合区间
     */
    @UserFunction("scypher.overlaps")
    @Description("Do OVERLAPS operation between two intervals.")
    public boolean overlaps(@Name("interval1") Map<String, Object> interval1, @Name("interval2") Map<String, Object> interval2) {
        if (interval1 != null && interval2 != null) {
            return (new SInterval(interval1)).overlaps(new SInterval(interval2));
        } else {
            throw new RuntimeException("Missing parameter interval1 or interval2.");
        }
    }
}

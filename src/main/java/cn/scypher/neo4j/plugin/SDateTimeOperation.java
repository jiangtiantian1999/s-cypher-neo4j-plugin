package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;

public class SDateTimeOperation {

    /**
     * @param timePoint 一个时间点，为str类型、Map类型或时间点类型
     * @return 返回当前时区和当前时间点类型下的时间点，为时间点类型
     */
    @UserFunction("scypher.timePoint")
    @Description("Get a time point.")
    public Object timePoint(@Name("timePoint") Object timePoint) {
        if (timePoint != null) {
            if (timePoint instanceof String | timePoint instanceof Map) {
                // timePoint为str或Map类型，获取用户之前所设置的时间点类型和时区
                String timePointType = GlobalVariablesManager.getTimePointType();
                String timezone = GlobalVariablesManager.getTimezone();
                return (new STimePoint(timePoint, timePointType, timezone)).getSystemTimePoint();
            } else if (timePoint instanceof STimePoint) {
                // timePoint为时间点类型
                return (new STimePoint(timePoint)).getSystemTimePoint();
            } else {
                throw new RuntimeException("Type mismatch: expected String, Map, Date, Time, LocalTime, LocalDateTime or DateTime but was " + timePoint.getClass().getSimpleName());
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param intervalFromObject 开始时间点，为str类型、Map类型或时间点类型
     * @param intervalToObject   结束时间点，为str类型、Map类型或时间点类型
     * @return 返回Map类型（具有两个key：from和to，value为时间点类型）
     */
    @UserFunction("scypher.interval")
    @Description("Set an interval.")
    public Map<String, Object> interval(@Name("intervalFrom") Object intervalFromObject, @Name("intervalTo") Object intervalToObject) {
        if (intervalFromObject != null && intervalToObject != null) {
            String timePointType = GlobalVariablesManager.getTimePointType();
            String timezone = GlobalVariablesManager.getTimezone();
            SInterval interval = new SInterval(intervalFromObject, intervalToObject, timePointType, timezone);
            return interval.getSystemInterval();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param intervalMapList 时间区间的列表
     * @return 返回列表中所有时间区间的交集。若不为空，返回返回Map类型（具有两个key：from和to，value为时间点类型）；若为空，返回NULL
     */
    @UserFunction("scypher.interval.intersection")
    @Description("Get the intersection of all intervals in a List.")
    public Map<String, Object> intervalIntersection(@Name("intervalList") List<Map<String, Object>> intervalMapList) {
        if (intervalMapList != null) {
            if (intervalMapList.size() > 0) {
                SInterval interval = new SInterval(intervalMapList.get(0));
                for (int index = 1; index < intervalMapList.size() && interval != null; index++) {
                    interval = interval.intersection(new SInterval(intervalMapList.get(index)));
                }
                if (interval != null) {
                    return interval.getSystemInterval();
                } else {
                    return null;
                }
            } else {
                throw new RuntimeException("The list of interval can't be empty");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param intervalMapList
     * @return 返回一个时间区间，这个区间包含列表中的所有时间区间
     */
    @UserFunction("scypher.interval.range")
    @Description("Get a interval within which all intervals in List are located.")
    public Map<String, Object> intervalRange(@Name("intervalList") List<Map<String, Object>> intervalMapList) {
        if (intervalMapList != null) {
            if (intervalMapList.size() > 0) {
                SInterval interval = new SInterval(intervalMapList.get(0));
                for (int index = 1; index < intervalMapList.size() && interval != null; index++) {
                    interval = interval.range(new SInterval(intervalMapList.get(index)));
                }
                if (interval != null) {
                    return interval.getSystemInterval();
                } else {
                    return null;
                }
            } else {
                throw new RuntimeException("The list of interval can't be empty.");
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param earlierIntervalMap 一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @param latterIntervalMap  一个时间区间，为Map类型（具有两个key：from和to，value为时间点类型）
     * @return 返回两个时间区间的时间差
     */
    @UserFunction("scypher.interval.difference")
    @Description("Get the time difference between two intervals.")
    public TemporalAmount intervalDifference(@Name("interval1") Map<String, Object> earlierIntervalMap, @Name("interval2") Map<String, Object> latterIntervalMap) {
        if (earlierIntervalMap != null && latterIntervalMap != null) {
            SInterval earlierInterval = new SInterval(earlierIntervalMap);
            SInterval latterInterval = new SInterval(latterIntervalMap);
            return earlierInterval.difference(latterInterval).getDuration();
        } else {
            throw new RuntimeException("Missing parameter");
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
            throw new RuntimeException("Missing parameter");
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
            throw new RuntimeException("Missing parameter");
        }
    }
}

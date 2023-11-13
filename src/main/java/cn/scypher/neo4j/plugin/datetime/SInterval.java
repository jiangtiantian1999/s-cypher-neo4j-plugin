package cn.scypher.neo4j.plugin.datetime;

import java.util.HashMap;
import java.util.Map;

public class SInterval {
    private final STimePoint intervalFrom;
    private final STimePoint intervalTo;

    /**
     * @param intervalFrom 开始时间
     * @param intervalTo   结束时间
     */
    public SInterval(STimePoint intervalFrom, STimePoint intervalTo) {
        if (intervalFrom.getTimePointType().equals(intervalTo.getTimePointType())) {
            if (intervalFrom.isAfter(intervalTo)) {
                throw new RuntimeException("The start time can't be latter than the end time");
            }
            this.intervalFrom = intervalFrom;
            this.intervalTo = intervalTo;
        } else {
            throw new RuntimeException("The type of the start time point and the end time point is not same");
        }
    }

    /**
     * @param interval 为map类型（具有两个key：from和to，value为时间点类型）
     */
    public SInterval(Map<String, Object> interval) {
        if (interval.containsKey("from") && interval.containsKey("to")) {
            STimePoint intervalFrom = new STimePoint(interval.get("from"));
            STimePoint intervalTo = new STimePoint(interval.get("to"));
            if (intervalFrom.getTimePointType().equals(intervalTo.getTimePointType())) {
                if (intervalFrom.isAfter(intervalTo)) {
                    throw new RuntimeException("The start time can't be latter than the end time");
                }
                this.intervalFrom = intervalFrom;
                this.intervalTo = intervalTo;
            } else {
                throw new RuntimeException("The type of the start time point and the end time point is not same");
            }
        } else {
            throw new RuntimeException("Missing key 'from' or 'to'");
        }
    }

    /**
     * @param intervalFromObject 开始时间的输入，为str类型、Map类型或时间点类型
     * @param intervalToObject   结束时间的输入，为str类型、Map类型或时间点类型
     * @param timePointType      时间点类型
     * @param timezone           默认时区
     */
    public SInterval(Object intervalFromObject, Object intervalToObject, String timePointType, String timezone) {
        STimePoint intervalFrom, intervalTo;
        if (intervalFromObject instanceof String | intervalFromObject instanceof Map) {
            intervalFrom = new STimePoint(intervalFromObject, timePointType, timezone);
        } else {
            // 开始时间为时间点类型
            intervalFrom = new STimePoint(intervalFromObject);
        }
        if (intervalToObject instanceof String | intervalToObject instanceof Map) {
            intervalTo = new STimePoint(intervalToObject, timePointType, timezone);
        } else {
            // 结束为时间点类型
            intervalTo = new STimePoint(intervalToObject);
        }
        if (intervalFrom.getTimePointType().equals(intervalTo.getTimePointType())) {
            if (intervalFrom.isAfter(intervalTo)) {
                throw new RuntimeException("The start time can't be latter than the end time");
            }
            this.intervalFrom = intervalFrom;
            this.intervalTo = intervalTo;
        } else {
            throw new RuntimeException("The type of the start time point and the end time point is not same");
        }
    }

    public boolean overlaps(SInterval interval) {
        if (this.getTimePointType().equals(interval.getTimePointType())) {
            return !(this.intervalFrom.isAfter(interval.getIntervalTo()) | interval.getIntervalFrom().isAfter(this.intervalTo));
        } else {
            throw new RuntimeException("Only the intervals of the same time point type can perform overlaps operations");
        }
    }

    public SInterval intersection(SInterval interval) {
        if (this.getTimePointType().equals(interval.getTimePointType())) {
            if (this.overlaps(interval)) {
                STimePoint interval_from = this.intervalFrom.isAfter(interval.getIntervalFrom()) ? this.intervalFrom : interval.getIntervalFrom();
                STimePoint interval_to = this.intervalTo.isBefore(interval.getIntervalTo()) ? this.intervalTo : interval.getIntervalTo();
                return new SInterval(interval_from, interval_to);
            }
            return null;
        } else {
            throw new RuntimeException("Only the intervals of the same time point type can perform intersection operations");
        }
    }

    public SInterval range(SInterval interval) {
        if (this.getTimePointType().equals(interval.getTimePointType())) {
            STimePoint intervalFrom, intervalTo;
            if (this.intervalFrom.isBefore(interval.getIntervalFrom())) {
                intervalFrom = this.intervalFrom;
            } else {
                intervalFrom = interval.getIntervalFrom();
            }
            if (this.intervalTo.isAfter(interval.getIntervalTo())) {
                intervalTo = this.intervalTo;
            } else {
                intervalTo = interval.getIntervalTo();
            }
            return new SInterval(intervalFrom, intervalTo);
        } else {
            throw new RuntimeException("Only the intervals of the same time point type can perform range operations");
        }
    }

    public SDuration difference(SInterval interval) {
        if (this.getTimePointType().equals(interval.getTimePointType())) {
            if (!this.overlaps(interval)) {
                if (this.intervalTo.isBefore(interval.getIntervalFrom())) {
                    return this.intervalTo.difference(interval.getIntervalFrom());
                } else {
                    return this.intervalFrom.difference(interval.getIntervalTo());
                }
            } else {
                throw new RuntimeException("The intervals overlap each other");
            }
        } else {
            throw new RuntimeException("Only the intervals of the same time point type can perform difference operations");
        }
    }

    public String getTimePointType() {
        return this.intervalFrom.getTimePointType();
    }

    public boolean contains(STimePoint timePoint) {
        if (this.intervalFrom.getTimePointType().equals(timePoint.getTimePointType())) {
            return !(this.intervalFrom.isAfter(timePoint) | this.intervalTo.isBefore(timePoint));
        } else {
            throw new RuntimeException("Only the interval can only contain the time point of the same type");
        }
    }

    public boolean contains(SInterval interval) {
        if (this.intervalFrom.getTimePointType().equals(interval.getTimePointType())) {
            return !interval.intervalFrom.isBefore(this.intervalFrom) && !interval.intervalTo.isAfter(this.intervalTo);
        } else {
            throw new RuntimeException("Only the interval can only contain the time point of the same type");
        }
    }

    public Map<String, Object> getSystemInterval() {
        Map<String, Object> interval = new HashMap<>();
        interval.put("from", this.intervalFrom.getSystemTimePoint());
        interval.put("to", this.intervalTo.getSystemTimePoint());
        return interval;
    }

    public STimePoint getIntervalFrom() {
        return this.intervalFrom;
    }

    public STimePoint getIntervalTo() {
        return this.intervalTo;
    }
}

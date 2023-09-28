package cn.scypher.neo4j.plugin.datetime;

public class SInterval {
    private final TimePoint intervalFrom;
    private final TimePoint intervalTo;

    public SInterval(TimePoint intervalFrom, TimePoint intervalTo) {
        if (intervalFrom.getClass().toString().equals(intervalTo.getClass().toString())) {
            if (intervalFrom.isAfter(intervalTo)) {
                throw new RuntimeException("The start time cannot be later than the end time.");
            }
            this.intervalFrom = intervalFrom;
            this.intervalTo = intervalTo;
        } else {
            throw new RuntimeException("The type of interval.from and interval.to is not same.");
        }
    }

    /**
     * @param intervalFromObject 开始时间的输入，为string或Map类型（匹配字符串或json）
     * @param intervalToObject   结束时间的输入，为string或Map类型（匹配字符串或json）
     * @param timePointType      时间点类型
     * @param timezone           默认时区
     */
    public SInterval(Object intervalFromObject, Object intervalToObject, String timePointType, String timezone) {
        TimePoint intervalFrom = new TimePoint(intervalFromObject, timePointType, timezone);
        TimePoint intervalTo = new TimePoint(intervalToObject, timePointType, timezone);
        if (intervalFrom.isAfter(intervalTo)) {
            throw new RuntimeException("The start time cannot be later than the end time.");
        }
        this.intervalFrom = intervalFrom;
        this.intervalTo = intervalTo;
    }

    public boolean overlaps(SInterval interval) {
        if (this.intervalFrom.getClass().toString().equals(interval.getIntervalFrom().getClass().toString())) {
            return !(this.intervalFrom.isAfter(interval.getIntervalTo()) | interval.getIntervalFrom().isAfter(this.intervalTo));
        } else {
            throw new RuntimeException("Only the intervals of the same time point type can perform overlaps operations.");
        }
    }

    public SInterval intersection(SInterval interval) {
        if (this.intervalFrom.getClass().toString().equals(interval.getIntervalFrom().getClass().toString())) {
            if (this.overlaps(interval)) {
                TimePoint interval_from = this.intervalFrom.isAfter(interval.getIntervalFrom()) ? this.intervalFrom : interval.getIntervalFrom();
                TimePoint interval_to = this.intervalTo.isBefore(interval.getIntervalTo()) ? this.intervalTo : interval.getIntervalTo();
                return new SInterval(interval_from, interval_to);
            }
        } else {
            throw new RuntimeException("Only the intervals of the same time point type can perform overlaps operations.");
        }
        return null;
    }

    public boolean contains(TimePoint timePoint) {
        if (this.intervalFrom.getClass().toString().equals(timePoint.getClass().toString())) {
            return !(this.intervalFrom.isAfter(timePoint) | this.intervalTo.isBefore(timePoint));
        } else {
            throw new RuntimeException("Only the interval can only contain the time point of the same type.");
        }
    }

    private TimePoint getIntervalFrom() {
        return this.intervalFrom;
    }

    private TimePoint getIntervalTo() {
        return this.intervalTo;
    }
}

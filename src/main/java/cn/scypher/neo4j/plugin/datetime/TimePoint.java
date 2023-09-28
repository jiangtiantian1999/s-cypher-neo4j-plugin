package cn.scypher.neo4j.plugin.datetime;


import java.time.*;
import java.util.Map;

public class TimePoint {
    private final Object timePoint;


    /**
     * @param timePoint 为LocalDate、OffsetTime、LocalTime、ZonedDateTime或LocalDateTime类型
     */
    public TimePoint(Object timePoint) {
        if (timePoint instanceof LocalDate) {
            this.timePoint = new SDate((LocalDate) timePoint);
        } else if (timePoint instanceof OffsetTime) {
            this.timePoint = new STime((OffsetTime) timePoint);
        } else if (timePoint instanceof LocalTime) {
            this.timePoint = new SLocalTime((LocalTime) timePoint);
        } else if (timePoint instanceof ZonedDateTime) {
            this.timePoint = new SDateTime((ZonedDateTime) timePoint);
        } else if (timePoint instanceof LocalDateTime) {
            this.timePoint = new SLocalDateTime((LocalDateTime) timePoint);
        } else {
            throw new RuntimeException("The type of time point must be 'LocalDate', 'OffsetTime', 'LocalTime', 'ZonedDateTime' or 'LocalDateTime'.");
        }
    }

    /**
     * @param timePoint     时间点的输入，为string或Map类型（匹配字符串或json）
     * @param timePointType 时间点类型
     * @param timezone      默认时区
     */
    public TimePoint(Object timePoint, String timePointType, String timezone) {
        if (!(timePoint instanceof String) && !(timePoint instanceof Map)) {
            throw new RuntimeException("The input of time point must be String or Map.");
        }
        if (timePointType.equals(LocalDate.class.toString())) {
            if (timePoint.getClass().toString().equals(String.class.toString())) {
                this.timePoint = new SDate((String) timePoint);
            } else {
                this.timePoint = new SDate((Map<String, Integer>) timePoint);
            }
        } else if (timePointType.equals(OffsetTime.class.toString())) {
            if (timePoint.getClass().toString().equals(String.class.toString())) {
                this.timePoint = new STime((String) timePoint, timezone);
            } else {
                this.timePoint = new STime((Map<String, Object>) timePoint, timezone);
            }
        } else if (timePointType.equals(LocalTime.class.toString())) {
            if (timePoint.getClass().toString().equals(String.class.toString())) {
                this.timePoint = new SLocalTime((String) timePoint);
            } else {
                this.timePoint = new SLocalTime((Map<String, Integer>) timePoint);
            }
        } else if (timePointType.equals(ZonedDateTime.class.toString())) {
            if (timePoint.getClass().toString().equals(String.class.toString())) {
                this.timePoint = new SDateTime((String) timePoint, timezone);
            } else {
                this.timePoint = new SDateTime((Map<String, Object>) timePoint, timezone);
            }
        } else if (timePointType.equals(LocalDateTime.class.toString())) {
            if (timePoint.getClass().toString().equals(String.class.toString())) {
                this.timePoint = new SLocalDateTime((String) timePoint);
            } else {
                this.timePoint = new SLocalDateTime((Map<String, Integer>) timePoint);
            }
        } else {
            throw new RuntimeException("The type of time point must be 'LocalDate', 'OffsetTime', 'LocalTime', 'ZonedDateTime' or 'LocalDateTime'.");
        }
    }

    public boolean isBefore(TimePoint timePoint) {
        if (this.timePoint.getClass() == timePoint.getTimePoint().getClass()) {
            if (this.timePoint instanceof SDate) {
                return ((SDate) this.timePoint).isBefore((SDate) timePoint.getTimePoint());
            } else if (this.timePoint instanceof STime) {
                return ((STime) this.timePoint).isBefore((STime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SLocalTime) {
                return ((SLocalTime) this.timePoint).isBefore((SLocalTime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SDateTime) {
                return ((SDateTime) this.timePoint).isBefore((SDateTime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SLocalDateTime) {
                return ((SLocalDateTime) this.timePoint).isBefore((SLocalDateTime) timePoint.getTimePoint());
            }
        } else {
            throw new RuntimeException("Only the time points of the same type can be compared.");
        }
        return false;
    }

    public boolean isAfter(TimePoint timePoint) {
        if (this.timePoint.getClass() == timePoint.getTimePoint().getClass()) {
            if (this.timePoint instanceof SDate) {
                return ((SDate) this.timePoint).isAfter((SDate) timePoint.getTimePoint());
            } else if (this.timePoint instanceof STime) {
                return ((STime) this.timePoint).isAfter((STime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SLocalTime) {
                return ((SLocalTime) this.timePoint).isAfter((SLocalTime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SDateTime) {
                return ((SDateTime) this.timePoint).isAfter((SDateTime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SLocalDateTime) {
                return ((SLocalDateTime) this.timePoint).isAfter((SLocalDateTime) timePoint.getTimePoint());
            }
        } else {
            throw new RuntimeException("Only the time points of the same type can be compared.");
        }
        return false;
    }


    public static String getTimePointType(String timePointClass) {
        if (timePointClass.equals(LocalDate.class.toString())) {
            return "date";
        } else if (timePointClass.equals(OffsetTime.class.toString())) {
            return "time";
        } else if (timePointClass.equals(LocalTime.class.toString())) {
            return "localtime";
        } else if (timePointClass.equals(ZonedDateTime.class.toString())) {
            return "datetime";
        } else if (timePointClass.equals(LocalDateTime.class.toString())) {
            return "localdatetime";
        }
        return null;
    }

    /**
     *
     * @return 返回SDate、STime、SLocalTime、SDateTime或SLocalDateTime
     */
    public Object getTimePoint() {
        return this.timePoint;
    }




    /**
     *
     * @return 返回LocalDate、OffsetTime、LocalTime、ZonedDateTime或LocalDateTime
     */
    public Object getSystemTimePoint() {
        if (this.timePoint instanceof SDate) {
            return ((SDate) this.timePoint).getDate();
        } else if (this.timePoint instanceof STime) {
            return ((STime) this.timePoint).getTime();
        } else if (this.timePoint instanceof SLocalTime) {
            return ((SLocalTime) this.timePoint).getLocalTime();
        } else if (this.timePoint instanceof SDateTime) {
            return ((SDateTime) this.timePoint).getDateTime();
        } else if (this.timePoint instanceof SLocalDateTime) {
            return ((SLocalDateTime) this.timePoint).getLocalDateTime();
        }
        return null;
    }
}

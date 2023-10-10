package cn.scypher.neo4j.plugin.datetime;

import java.time.*;
import java.util.Map;

public class STimePoint {
    private final Object timePoint;


    /**
     * @param timePoint 为LocalDate、OffsetTime、LocalTime、ZonedDateTime或LocalDateTime类型
     */
    public STimePoint(Object timePoint) {
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
    public STimePoint(Object timePoint, String timePointType, String timezone) {
        if (timePoint instanceof String | timePoint instanceof Map) {
            if (timePointType.equals("date")) {
                if (timePoint instanceof String) {
                    this.timePoint = new SDate((String) timePoint);
                } else {
                    this.timePoint = new SDate((Map<String, Integer>) timePoint);
                }
            } else if (timePointType.equals("time")) {
                if (timePoint instanceof String) {
                    this.timePoint = new STime((String) timePoint, timezone);
                } else {
                    this.timePoint = new STime((Map<String, Object>) timePoint, timezone);
                }
            } else if (timePointType.equals("localtime")) {
                if (timePoint instanceof String) {
                    this.timePoint = new SLocalTime((String) timePoint);
                } else {
                    this.timePoint = new SLocalTime((Map<String, Integer>) timePoint);
                }
            } else if (timePointType.equals("datetime")) {
                if (timePoint instanceof String) {
                    this.timePoint = new SDateTime((String) timePoint, timezone);
                } else {
                    this.timePoint = new SDateTime((Map<String, Object>) timePoint, timezone);
                }
            } else if (timePointType.equals("localdatetime")) {
                if (timePoint instanceof String) {
                    this.timePoint = new SLocalDateTime((String) timePoint);
                } else {
                    this.timePoint = new SLocalDateTime((Map<String, Integer>) timePoint);
                }
            } else {
                throw new RuntimeException("The type of time point must be 'LocalDate', 'OffsetTime', 'LocalTime', 'ZonedDateTime' or 'LocalDateTime'.");
            }
        } else {
            throw new RuntimeException("The input of time point must be String or Map.");
        }
    }

    public SDuration difference(STimePoint timePoint) {
        if (this.getTimePointType().equals(timePoint.getTimePointType())) {
            if (this.timePoint instanceof SDate) {
                return ((SDate) this.timePoint).difference((SDate) timePoint.getTimePoint());
            } else if (this.timePoint instanceof STime) {
                return ((STime) this.timePoint).difference((STime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SLocalTime) {
                return ((SLocalTime) this.timePoint).difference((SLocalTime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SDateTime) {
                return ((SDateTime) this.timePoint).difference((SDateTime) timePoint.getTimePoint());
            } else if (this.timePoint instanceof SLocalDateTime) {
                return ((SLocalDateTime) this.timePoint).difference((SLocalDateTime) timePoint.getTimePoint());
            }
            return null;
        } else {
            throw new RuntimeException("Only the time points of the same type can make a difference.");
        }
    }

    public boolean isBefore(STimePoint timePoint) {
        if (this.getTimePointType().equals(timePoint.getTimePointType())) {
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
            return false;
        } else {
            throw new RuntimeException("Only the time points of the same type can be compared.");
        }
    }

    public boolean isAfter(STimePoint timePoint) {
        if (this.getTimePointType().equals(timePoint.getTimePointType())) {
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
            return false;
        } else {
            throw new RuntimeException("Only the time points of the same type can be compared.");
        }
    }

    public String getTimePointType() {
        if (this.timePoint instanceof SDate) {
            return "date";
        } else if (this.timePoint instanceof STime) {
            return "time";
        } else if (this.timePoint instanceof LocalTime) {
            return "localtime";
        } else if (this.timePoint instanceof SDateTime) {
            return "datetime";
        } else if (this.timePoint instanceof SLocalDateTime) {
            return "localdatetime";
        }
        return null;
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
     * @return 返回SDate、STime、SLocalTime、SDateTime或SLocalDateTime
     */
    public Object getTimePoint() {
        return this.timePoint;
    }

    /**
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

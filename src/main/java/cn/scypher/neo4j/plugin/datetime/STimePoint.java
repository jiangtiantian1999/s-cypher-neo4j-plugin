package cn.scypher.neo4j.plugin.datetime;

import java.time.*;
import java.util.Map;

public class STimePoint {
    // 为LocalDate、OffsetTime、LocalTime、ZonedDateTime或LocalDateTime类型
    private final Object timePoint;

    public STimePoint(String timePointType, String timezone) {
        switch (timePointType) {
            case "date" -> this.timePoint = new SDate();
            case "time" -> this.timePoint = new STime(timezone);
            case "localtime" -> this.timePoint = new SLocalTime();
            case "datetime" -> this.timePoint = new SDateTime(timezone);
            case "localdatetime" -> this.timePoint = new SLocalDateTime();
            default ->
                    throw new RuntimeException("The time point type must be date, time, localtime, datetime or localdatetime but was" + timePointType);
        }
    }

    /**
     * @param timePoint 为LocalDate、OffsetTime、LocalTime、ZonedDateTime或LocalDateTime类型
     *                  设置timePoint为SDate、STime、SLocalTime、SDateTime或SLocalDateTime类型的数据
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
            throw new RuntimeException("Type mismatch: expected LocalDate, OffsetTime, LocalTime, ZonedDateTime or LocalDateTime but was " + timePoint.getClass().getSimpleName());
        }
    }

    /**
     * @param timePoint     时间点的输入，为string或Map类型
     * @param timePointType 时间点类型
     * @param timezone      默认时区
     */
    public STimePoint(Object timePoint, String timePointType, String timezone) {
        if (timePoint instanceof String | timePoint instanceof Map) {
            switch (timePointType) {
                case "date" -> {
                    if (timePoint instanceof String) {
                        this.timePoint = new SDate((String) timePoint);
                    } else {
                        this.timePoint = new SDate((Map<String, Number>) timePoint);
                    }
                }
                case "time" -> {
                    if (timePoint instanceof String) {
                        this.timePoint = new STime((String) timePoint, timezone);
                    } else {
                        this.timePoint = new STime((Map<String, Object>) timePoint, timezone);
                    }
                }
                case "localtime" -> {
                    if (timePoint instanceof String) {
                        this.timePoint = new SLocalTime((String) timePoint);
                    } else {
                        this.timePoint = new SLocalTime((Map<String, Number>) timePoint);
                    }
                }
                case "datetime" -> {
                    if (timePoint instanceof String) {
                        this.timePoint = new SDateTime((String) timePoint, timezone);
                    } else {
                        this.timePoint = new SDateTime((Map<String, Object>) timePoint, timezone);
                    }
                }
                case "localdatetime" -> {
                    if (timePoint instanceof String) {
                        this.timePoint = new SLocalDateTime((String) timePoint);
                    } else {
                        this.timePoint = new SLocalDateTime((Map<String, Number>) timePoint);
                    }
                }
                default ->
                        throw new RuntimeException("The time point type must be date, time, localtime, datetime or localdatetime but was" + timePointType);
            }
        } else {
            throw new RuntimeException("Type mismatch: expected String or Map but was " + timePoint.getClass().getSimpleName());
        }
    }

    public Duration difference(STimePoint timePoint) {
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
            throw new RuntimeException("Only the time points of the same type can make a difference");
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
            throw new RuntimeException("Only the time points of the same type can be compared");
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
            throw new RuntimeException("Only the time points of the same type can be compared");
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
        } else {
            return "localdatetime";
        }
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

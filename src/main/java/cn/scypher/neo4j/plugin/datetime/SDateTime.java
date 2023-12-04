package cn.scypher.neo4j.plugin.datetime;

import java.time.*;
import java.util.HashMap;
import java.util.Map;

public class SDateTime {
    private final ZonedDateTime datetime;

    public SDateTime(String timezone) {
        this.datetime = ZonedDateTime.now(ZoneOffset.of(timezone));
    }

    public SDateTime(ZonedDateTime datetime) {
        this.datetime = datetime;
    }

    public SDateTime(String datetimeString, String timezone) {
        if (timezone == null) {
            timezone = "Z";
        }
        if (datetimeString.equalsIgnoreCase("NOW")) {
            this.datetime = ZonedDateTime.of(LocalDateTime.MAX, ZoneOffset.of(timezone)).withYear(9999);
        } else {
            String[] datetimeStringList = datetimeString.split("T");
            SDate date = new SDate(datetimeStringList[0]);
            if (datetimeStringList.length == 2) {
                STime time = new STime(datetimeStringList[1], timezone);
                this.datetime = ZonedDateTime.of(date.getDate(), time.getTime().toLocalTime(), time.getTime().getOffset());
            } else {
                this.datetime = ZonedDateTime.of(date.getDate(), LocalTime.MIN, ZoneOffset.of(timezone));
            }
        }
    }

    public SDateTime(Map<String, Object> datetimeMap, String timezone) {
        String[] dateComponents = {"year", "month", "day", "week", "dayOfWeek", "quarter", "dayOfQuarter", "ordinalDay"};
        Map<String, Number> dateMap = new HashMap<>();
        for (String component : dateComponents) {
            if (datetimeMap.containsKey(component)) {
                dateMap.put(component, (Number) datetimeMap.get(component));
            }
        }
        SDate date = new SDate(dateMap);
        STime time;
        if (datetimeMap.containsKey("hour") | datetimeMap.containsKey("minute") | datetimeMap.containsKey("second")
                | datetimeMap.containsKey("millisecond") | datetimeMap.containsKey("microsecond") | datetimeMap.containsKey("nanosecond")) {
            time = new STime(datetimeMap, timezone);
        } else {
            time = new STime("00", timezone);
        }
        this.datetime = ZonedDateTime.of(date.getDate(), time.getTime().toLocalTime(), time.getTime().getOffset());
    }

    public Duration difference(SDateTime datetime) {
        return Duration.between(this.datetime, datetime.getDateTime());
    }

    public boolean isBefore(SDateTime datetime) {
        return this.datetime.isBefore(datetime.getDateTime());
    }

    public boolean isAfter(SDateTime datetime) {
        return this.datetime.isAfter(datetime.getDateTime());
    }

    public ZonedDateTime getDateTime() {
        return this.datetime;
    }
}

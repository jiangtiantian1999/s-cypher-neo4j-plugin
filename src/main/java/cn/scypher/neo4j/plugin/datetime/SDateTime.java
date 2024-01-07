package cn.scypher.neo4j.plugin.datetime;

import java.time.*;
import java.util.HashMap;
import java.util.Map;

public class SDateTime {
    private final ZonedDateTime datetime;

    public final ZonedDateTime MIN;
    public final ZonedDateTime MAX;

    public SDateTime(String timezone) {
        if (timezone == null) {
            timezone = "Z";
        }
        this.datetime = ZonedDateTime.now(ZoneId.of(timezone));
        this.MIN = ZonedDateTime.of(LocalDateTime.MIN, ZoneId.of(timezone));
        this.MAX = ZonedDateTime.of(LocalDateTime.MAX, ZoneId.of(timezone));
    }

    public SDateTime(ZonedDateTime datetime) {
        this.datetime = datetime;
        this.MIN = ZonedDateTime.of(LocalDateTime.MIN, datetime.getZone());
        this.MAX = ZonedDateTime.of(LocalDateTime.MAX, datetime.getZone());
    }

    public SDateTime(String datetimeString, String timezone) {
        if (timezone == null) {
            timezone = "Z";
        }
        if (datetimeString.equalsIgnoreCase("NOW")) {
            this.datetime = ZonedDateTime.of(LocalDateTime.MAX, ZoneId.of(timezone)).withYear(9999);
        } else {
            String[] datetimeStringList = datetimeString.split("T");
            SDate date = new SDate(datetimeStringList[0]);
            if (datetimeStringList.length == 2) {
                STime time = new STime(datetimeStringList[1], timezone);
                this.datetime = ZonedDateTime.of(date.getDate(), time.getTime().toLocalTime(), time.getTime().getOffset());
            } else {
                this.datetime = ZonedDateTime.of(date.getDate(), LocalTime.MIN, ZoneId.of(timezone));
            }
        }
        this.MIN = ZonedDateTime.of(LocalDateTime.MIN, ZoneId.of(timezone));
        this.MAX = ZonedDateTime.of(LocalDateTime.MAX, ZoneId.of(timezone));
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
        if (timezone == null) {
            timezone = "Z";
        }
        this.MIN = ZonedDateTime.of(LocalDateTime.MIN, ZoneId.of(timezone));
        System.out.println(this.MIN);
        this.MAX = ZonedDateTime.of(LocalDateTime.MAX, ZoneId.of(timezone));
        System.out.println(this.MAX);
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

package cn.scypher.neo4j.plugin.datetime;

import java.time.*;
import java.util.HashMap;
import java.util.Map;

public class SDateTime {
    private final ZonedDateTime datetime;

    public SDateTime() {
        this.datetime = ZonedDateTime.now(ZoneOffset.UTC);
    }

    public SDateTime(ZonedDateTime datetime) {
        this.datetime = datetime;
    }

    public SDateTime(String datetimeString, String timezone) {
        if (datetimeString.equalsIgnoreCase("NOW")) {
            if (timezone == null) {
                this.datetime = ZonedDateTime.of(LocalDateTime.MAX, ZoneOffset.UTC);
            } else {
                this.datetime = ZonedDateTime.now(ZoneOffset.of(timezone));
            }
        } else {
            String[] datetimeStringList = datetimeString.split("T");
            if (datetimeStringList.length == 2) {
                SDate date = new SDate(datetimeStringList[0]);
                STime time = new STime(datetimeStringList[1], timezone);
                this.datetime = ZonedDateTime.of(date.getDate(), time.getTime().toLocalTime(), time.getTime().getOffset());
            } else {
                throw new RuntimeException("The format of the datetime string is incorrect.");
            }
        }
    }

    public SDateTime(Map<String, Object> datetimeMap, String timezone) {
        String[] dateComponents = {"year", "month", "day", "week", "dayOfWeek", "quarter", "dayOfQuarter", "ordinalDay"};
        Map<String, Integer> dateMap = new HashMap<>();
        for (String component : dateComponents) {
            if (datetimeMap.containsKey(component)) {
                dateMap.put(component, (Integer) datetimeMap.get(component));
            }
        }
        SDate date = new SDate(dateMap);
        STime time = new STime(datetimeMap, timezone);
        this.datetime = ZonedDateTime.of(date.getDate(), time.getTime().toLocalTime(), time.getTime().getOffset());
    }

    public boolean isBefore(SDateTime timePoint) {
        return this.datetime.isBefore(timePoint.getDateTime());
    }

    public boolean isAfter(SDateTime timePoint) {
        return this.datetime.isAfter(timePoint.getDateTime());
    }

    public ZonedDateTime getDateTime() {
        return this.datetime;
    }
}

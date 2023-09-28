package cn.scypher.neo4j.plugin.datetime;

import java.time.LocalDateTime;
import java.util.Map;

public class SLocalDateTime {
    private final LocalDateTime localdatetime;

    public SLocalDateTime() {
        this.localdatetime = LocalDateTime.now();
    }

    public SLocalDateTime(LocalDateTime localDateTime) {
        this.localdatetime = localDateTime;
    }

    public SLocalDateTime(String localdatetimeString) {
        if (localdatetimeString.equalsIgnoreCase("NOW")) {
            this.localdatetime = LocalDateTime.MAX;
        } else {
            String[] localdatetimeStringList = localdatetimeString.split("T");
            if (localdatetimeStringList.length == 2) {
                SDate date = new SDate(localdatetimeStringList[0]);
                SLocalTime time = new SLocalTime(localdatetimeStringList[1]);
                this.localdatetime = LocalDateTime.of(date.getDate(), time.getLocalTime());
            } else {
                throw new RuntimeException("The format of the localdatetime string is incorrect.");
            }
        }
    }

    public SLocalDateTime(Map<String, Integer> datetimeMap) {
        SDate date = new SDate(datetimeMap);
        SLocalTime localtime = new SLocalTime(datetimeMap);
        this.localdatetime = LocalDateTime.of(date.getDate(), localtime.getLocalTime());
    }

    public boolean isBefore(SLocalDateTime timePoint) {
        return this.localdatetime.isBefore(timePoint.getLocalDateTime());
    }

    public boolean isAfter(SLocalDateTime timePoint) {
        return this.localdatetime.isAfter(timePoint.getLocalDateTime());
    }

    public LocalDateTime getLocalDateTime() {
        return this.localdatetime;
    }
}

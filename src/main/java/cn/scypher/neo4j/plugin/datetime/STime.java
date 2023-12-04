package cn.scypher.neo4j.plugin.datetime;

import java.time.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class STime {
    private final OffsetTime time;

    public STime(String timezone) {
        this.time = OffsetTime.now(ZoneOffset.of(timezone));
    }

    public STime(OffsetTime time) {
        this.time = time;
    }

    public STime(String timeString, String timezone) {
        if (timezone == null) {
            timezone = "Z";
        }
        if (timeString.equalsIgnoreCase("NOW")) {
            this.time = OffsetTime.of(LocalTime.MAX, ZoneOffset.of(timezone));
        } else {
            Pattern localtimePattern = Pattern.compile("(?<hour>\\d{2})(:?(?<minute>\\d{2})((:?(?<second>\\d{2}))((.|,)(?<nanosecond>\\d{1,9}))?)?)?");
            Pattern timezonePattern = Pattern.compile("(?<Z>Z)|(\\[(?<zoneName1>[\\w/]+)\\])|(((?<plus>\\+)|(?<minus>-))(?<hours>\\d{2})(:?(?<minutes>\\d{2}))?(\\[(?<zoneName2>[\\w/]+)\\])?)");
            Pattern timePattern = Pattern.compile("T?(?<time>(" + localtimePattern + "))[ ]*(?<timezone>(" + timezonePattern + "))?");
            Matcher matcher = timePattern.matcher(timeString.trim());
            Map<String, Integer> timeMap = new HashMap<>();
            String[] timeComponents = {"hour", "minute", "second", "nanosecond"};
            if (matcher.find()) {
                for (String component : timeComponents) {
                    if (matcher.group(component) != null) {
                        if (component.equals("nanosecond")) {
                            timeMap.put(component, Integer.parseInt(String.format("%-9s", matcher.group(component)).replace(" ", "0")));
                        } else {
                            timeMap.put(component, Integer.parseInt(matcher.group(component)));
                        }
                    }
                }
                if (matcher.group("timezone") != null) {
                    this.time = this.parseTimeMap(timeMap, matcher.group("timezone"));
                } else {
                    this.time = this.parseTimeMap(timeMap, timezone);
                }
            } else {
                throw new RuntimeException("The combination of the time components is incorrect.");
            }
        }

    }

    public STime(Map<String, Object> timeMap, String timezone) {
        // 至少指定hour或timezone
        if (!timeMap.containsKey("hour") && !timeMap.containsKey("timezone")) {
            throw new RuntimeException("The combination of the time components is incorrect.");
        }
        // 不能跨过粗粒度的时间单位指定细粒度的时间单位
        if (((timeMap.containsKey("nanosecond") | timeMap.containsKey("microsecond") | timeMap.containsKey("millisecond")) && !timeMap.containsKey("second"))
                | (timeMap.containsKey("second") && !timeMap.containsKey("minute"))
                | (timeMap.containsKey("minute") && !timeMap.containsKey("hour"))) {
            throw new RuntimeException("The combination of the time components is incorrect.");
        }
        String[] timeComponents = {"hour", "minute", "second", "millisecond", "microsecond", "nanosecond"};
        Map<String, Integer> timeIntegerMap = new HashMap<>();
        for (String component : timeComponents) {
            if (timeMap.containsKey(component)) {
                timeIntegerMap.put(component, (Integer) timeMap.get(component));
            }
        }
        if (timeMap.containsKey("timezone")) {
            this.time = this.parseTimeMap(timeIntegerMap, (String) timeMap.get("timezone"));
        } else {
            this.time = this.parseTimeMap(timeIntegerMap, timezone);
        }
    }

    public Duration difference(STime time) {
        return Duration.between(this.time, time.getTime());
    }

    public boolean isBefore(STime time) {
        return this.time.isBefore(time.getTime());
    }

    public boolean isAfter(STime time) {
        return this.time.isAfter(time.getTime());
    }


    public OffsetTime getTime() {
        return this.time;
    }

    private OffsetTime parseTimeMap(Map<String, Integer> timeMap, String timezone) {
        int hour = timeMap.getOrDefault("hour", 0);
        int minute = timeMap.getOrDefault("minute", 0);
        int second = timeMap.getOrDefault("second", 0);
        int millisecond = timeMap.getOrDefault("millisecond", 0);
        int microsecond = timeMap.getOrDefault("microsecond", 0);
        int nanosecond = timeMap.getOrDefault("nanosecond", 0) + millisecond * 1000000 + microsecond * 1000;
        if (timezone != null) {
            return OffsetTime.of(hour, minute, second, nanosecond, ZoneOffset.of(timezone));
        } else {
            return OffsetTime.of(hour, minute, second, nanosecond, ZoneOffset.UTC);
        }
    }
}

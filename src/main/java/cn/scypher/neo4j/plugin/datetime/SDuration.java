package cn.scypher.neo4j.plugin.datetime;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SDuration {
    private Duration duration;

    public SDuration() {
        this.duration = Duration.ZERO;
    }

    public SDuration(Duration duration) {
        this.duration = duration;
    }

    public SDuration(String durationString) {
        Pattern durationPattern = Pattern.compile("P?((?<weeks>\\d{1,64})W)?((?<days>\\d{1,64})D)?((?<hours>\\d{1,64})H)?((?<minutes>\\d{1,64})M)?((?<seconds>\\d{1,64})(.|,)(?<nanoseconds>\\d{1,9})S)?");
        Matcher matcher = durationPattern.matcher(durationString.trim());
        Map<String, Integer> durationMap = new HashMap<>();
        String[] durationComponents = {"weeks", "days", "hours", "minutes", "seconds", "nanoseconds"};
        if (matcher.find()) {
            for (String component : durationComponents) {
                if (matcher.group(component) != null) {
                    if (component.equals("nanoseconds")) {
                        durationMap.put(component, Integer.parseInt(String.format("%-9s", matcher.group(component)).replace(" ", "0")));
                    } else {
                        durationMap.put(component, Integer.parseInt(matcher.group(component)));
                    }
                }
            }
            this.duration = this.parseDurationMap(durationMap);
        } else {
            throw new RuntimeException("The combination of the date components is incorrect.");
        }
    }

    public SDuration(Map<String, Integer> durationMap) {
        this.duration = this.parseDurationMap(durationMap);
    }

    public Duration getDuration() {
        return this.duration;
    }

    private Duration parseDurationMap(Map<String, Integer> durationMap) {
        long nanoseconds = 0;
        if (durationMap.containsKey("weeks")) {
            nanoseconds += (long) durationMap.get("weeks") * 7 * 24 * 60 * 60 * 1000000000;
        }
        if (durationMap.containsKey("days")) {
            nanoseconds += (long) durationMap.get("days") * 24 * 60 * 60 * 1000000000;
        }
        if (durationMap.containsKey("hours")) {
            nanoseconds += (long) durationMap.get("hours") * 60 * 60 * 1000000000;
        }
        if (durationMap.containsKey("minutes")) {
            nanoseconds += (long) durationMap.get("minutes") * 60 * 1000000000;
        }
        if (durationMap.containsKey("seconds")) {
            nanoseconds += (long) durationMap.get("seconds") * 1000000000;
        }
        if (durationMap.containsKey("nanoseconds")) {
            nanoseconds += (long) durationMap.get("nanoseconds");
        }
        return Duration.ofNanos(nanoseconds);
    }
}

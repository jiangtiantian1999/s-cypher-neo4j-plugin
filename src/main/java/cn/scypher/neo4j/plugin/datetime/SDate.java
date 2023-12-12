package cn.scypher.neo4j.plugin.datetime;

import javax.swing.*;
import java.time.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SDate {
    private final LocalDate date;

    public SDate() {
        this.date = LocalDate.now();
    }

    public SDate(LocalDate date) {
        this.date = date;
    }

    public SDate(String dateString) {
        if (dateString.equalsIgnoreCase("NOW")) {
            this.date = LocalDate.MAX.withYear(9999);
        } else {
            Pattern datePattern = Pattern.compile("((?<year>\\d{4})|(?<beyondYear>((\\+)|-)\\d{1,9}))((-?(?<ordinalDay>\\d{3})$)|" +
                    "(-?(?<month>\\d{1,2})(-?(?<day>\\d{1,2})?))|" +
                    "(-?W(?<week>\\d{1,2})(-?(?<dayOfWeek>\\d))?)|" +
                    "(-?Q(?<quarter>\\d)(-?(?<dayOfQuarter>\\d{1,2}))?))?");
            Matcher matcher = datePattern.matcher(dateString.trim());
            Map<String, Number> dateMap = new HashMap<>();
            String[] dateComponents = {"year", "beyondYear", "month", "day", "week", "dayOfWeek", "quarter", "dayOfQuarter", "ordinalDay"};
            if (matcher.find()) {
                for (String component : dateComponents) {
                    if (matcher.group(component) != null) {
                        if (component.equals("beyondYear")) {
                            dateMap.put("year", Integer.parseInt(matcher.group(component)));
                        }
                        dateMap.put(component, Integer.parseInt(matcher.group(component)));
                    }
                }
                this.date = this.parseDateMap(dateMap);
            } else {
                throw new RuntimeException("The combination of the date components is incorrect.");
            }
        }

    }

    public SDate(Map<String, Number> dateMap) {
        // 至少指定year
        if (dateMap.containsKey("year")) {
            if ((dateMap.containsKey("month") ? 1 : 0) + (dateMap.containsKey("week") ? 1 : 0) + (dateMap.containsKey("quarter") ? 1 : 0) + (dateMap.containsKey("ordinalDay") ? 1 : 0) > 1) {
                throw new RuntimeException("The combination of the date components is incorrect.");
            }
            if ((dateMap.containsKey("day") && !dateMap.containsKey("month"))
                    | (dateMap.containsKey("dayOfWeek") && !dateMap.containsKey("week"))
                    | (dateMap.containsKey("dayOfQuarter") && !dateMap.containsKey("quarter"))) {
                throw new RuntimeException("The combination of the date components is incorrect.");
            }
            this.date = this.parseDateMap(dateMap);
        } else {
            throw new RuntimeException("The combination of the date components is incorrect.");
        }
    }

    public Duration difference(SDate date) {
        return Duration.between(this.date, date.getDate());
    }

    public boolean isBefore(SDate date) {
        return this.date.isBefore(date.getDate());
    }

    public boolean isAfter(SDate date) {
        return this.date.isAfter(date.getDate());
    }

    public LocalDate getDate() {
        return this.date;
    }

    private LocalDate parseDateMap(Map<String, Number> dateMap) {
        int year = dateMap.get("year").intValue();
        if (dateMap.containsKey("month")) {
            int month = dateMap.get("month").intValue();
            int day = dateMap.getOrDefault("day", 1).intValue();
            return LocalDate.of(year, month, day);
        } else if (dateMap.containsKey("week")) {
            int week = dateMap.get("week").intValue();
            int dayOfWeek = dateMap.getOrDefault("dayOfWeek", 1).intValue();
            Calendar calendar = Calendar.getInstance();
            int[] weekdays = {Calendar.MONDAY, Calendar.TUESDAY, Calendar.WEDNESDAY, Calendar.THURSDAY, Calendar.FRIDAY, Calendar.SATURDAY, Calendar.SUNDAY};
            calendar.setWeekDate(year, week, weekdays[dayOfWeek - 1]);
            return LocalDate.of(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH));
        } else if (dateMap.containsKey("quarter")) {
            int quarter = dateMap.get("quarter").intValue();
            int dayOfQuarter = 1;
            if (dateMap.containsKey("dayOfQuarter")) {
                dayOfQuarter = dateMap.get("dayOfQuarter").intValue();
            }
            // 将一年中的第q个季节的第d天，转换为一年中的m月n日
            // 获取该年2月的天数
            Calendar calendar = Calendar.getInstance();
            calendar.set(year, Calendar.FEBRUARY, 1);
            int febDays = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
            int[] monthLength = {31, febDays + 31, febDays + 62, 30, 61, 91, 31, 62, 92, 31, 61, 92};
            int month = new int[]{1, 4, 7, 10}[quarter - 1];
            int dayOfMonth;
            if (dayOfQuarter <= monthLength[month - 1]) {
                dayOfMonth = dayOfQuarter;
            } else if (dayOfQuarter <= monthLength[month]) {
                dayOfMonth = dayOfQuarter - monthLength[month - 1];
                month += 1;
            } else if (dayOfQuarter <= monthLength[month + 1]) {
                dayOfMonth = dayOfQuarter - monthLength[month];
                month += 2;
            } else {
                throw new RuntimeException("The day of quarter must be in 1..90/91/92");
            }
            return LocalDate.of(year, month, dayOfMonth);
        } else if (dateMap.containsKey("ordinalDay")) {
            return LocalDate.ofYearDay(year, dateMap.get("ordinalDay").intValue());
        }
        return LocalDate.of(year, 1, 1);
    }
}


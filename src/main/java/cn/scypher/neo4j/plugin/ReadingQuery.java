package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class ReadingQuery {

    public static Node getPropertyNode(Node objectNode, String propertyName) {
        ResourceIterable<Relationship> relationships = objectNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("OBJECT_PROPERTY"));
        for (Relationship relationship : relationships) {
            Node endNode = relationship.getEndNode();
            if (endNode.getProperty("content").equals(propertyName)) {
                return endNode;
            }
        }
        return null;
    }

    /**
     * @param propertyNode 属性节点
     * @param timeWindow   时间点/时间区间
     * @return 返回在时间窗口上有效的所有值节点
     */
    public static List<Node> getValueNodes(Node propertyNode, @Name("timeWindow") Object timeWindow) {
        ResourceIterable<Relationship> relationships = propertyNode.getRelationships(Direction.OUTGOING, RelationshipType.withName("PROPERTY_VALUE"));
        List<Node> valueNodeList = new ArrayList<>();
        // snapshot/scope语句指定的时间区间
        STimePoint snapshotTimePoint = GlobalVariablesManager.getSnapshotTimePoint();
        SInterval scopeInterval = GlobalVariablesManager.getScopeInterval();
        STimePoint valueNodeTimePoint = null;
        SInterval valueNodeInterval = null;
        if (timeWindow != null) {
            if (timeWindow instanceof LocalDate | timeWindow instanceof OffsetTime | timeWindow instanceof LocalTime | timeWindow instanceof ZonedDateTime | timeWindow instanceof LocalDateTime) {
                valueNodeTimePoint = new STimePoint(timeWindow);
            } else if (timeWindow instanceof Map) {
                valueNodeInterval = new SInterval((Map<String, Object>) timeWindow);
            } else {
                throw new RuntimeException("Type mismatch: expected Date, Time, LocalTime, LocalDateTime, DateTime or Interval but was " + timeWindow.getClass().getSimpleName());
            }
        }
        for (Relationship relationship : relationships) {
            Node valueNode = relationship.getEndNode();
            SInterval valueNodeEffectiveTime = new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo")));
            if (valueNodeTimePoint == null && valueNodeInterval == null && snapshotTimePoint == null && scopeInterval == null) {
                valueNodeList.add(valueNode);
            } else if (valueNodeTimePoint != null && valueNodeEffectiveTime.contains(valueNodeTimePoint)) {
                valueNodeList.add(valueNode);
            } else if (valueNodeInterval != null && valueNodeEffectiveTime.overlaps(valueNodeInterval)) {
                valueNodeList.add(valueNode);
            } else if (valueNodeTimePoint == null && valueNodeInterval == null) {
                if (scopeInterval != null && valueNodeEffectiveTime.overlaps(scopeInterval)) {
                    // 时序查询子句和delete子句均优先使用scope定义的有效时间
                    valueNodeList.add(valueNode);
                } else if (snapshotTimePoint != null && valueNodeEffectiveTime.contains(snapshotTimePoint)) {
                    valueNodeList.add(valueNode);
                }
            }
        }
        return valueNodeList;
    }

    public int getComponentOfTimePoint(Date date, String component) {
        Calendar dateCalendar = Calendar.getInstance();
        dateCalendar.setTime(date);
        return switch (component) {
            case "year", "weekYear" -> dateCalendar.get(Calendar.YEAR);
            case "quarter" -> (dateCalendar.get(Calendar.MONTH)) / 4 + 1;
            case "month" -> dateCalendar.get(Calendar.MONTH) + 1;
            case "week" -> dateCalendar.get(Calendar.WEEK_OF_YEAR);
            case "dayOfQuarter" -> {
                int month = dateCalendar.get(Calendar.MONTH) + 1;
                if (month % 3 == 1) {
                    yield dateCalendar.get(Calendar.DAY_OF_MONTH);
                } else if (month % 3 == 2) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.set(dateCalendar.get(Calendar.YEAR), dateCalendar.get(Calendar.MONTH) - 1, 1);
                    yield calendar.getActualMaximum(Calendar.DAY_OF_MONTH) + dateCalendar.get(Calendar.DAY_OF_MONTH);
                } else {
                    Calendar calendar1 = Calendar.getInstance();
                    calendar1.set(dateCalendar.get(Calendar.YEAR), dateCalendar.get(Calendar.MONTH) - 2, 1);
                    Calendar calendar2 = Calendar.getInstance();
                    calendar2.set(dateCalendar.get(Calendar.YEAR), dateCalendar.get(Calendar.MONTH) - 1, 1);
                    yield calendar1.getActualMaximum(Calendar.DAY_OF_MONTH) + calendar2.getActualMaximum(Calendar.DAY_OF_MONTH) + dateCalendar.get(Calendar.DAY_OF_MONTH);
                }
            }
            case "quarterDay" -> {
                int quarter = (dateCalendar.get(Calendar.MONTH)) / 4 + 1;
                if (quarter == 1) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.set(dateCalendar.get(Calendar.YEAR), Calendar.FEBRUARY, 1);
                    int febDays = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
                    yield febDays + 62;
                } else if (quarter == 2) {
                    yield 91;
                } else {
                    yield 92;
                }
            }
            case "day" -> dateCalendar.get(Calendar.DAY_OF_MONTH);
            case "ordinalDay" -> dateCalendar.get(Calendar.DAY_OF_YEAR);
            case "dayOfWeek", "weekDay" -> {
                int[] weekdays = {7, 1, 2, 3, 4, 5, 6};
                yield weekdays[dateCalendar.get(Calendar.DAY_OF_WEEK) - 1];
            }
            default -> throw new RuntimeException("No such field: " + component);
        };
    }

    public int getComponentOfTimePoint(LocalTime localTime, String component) {
        return switch (component) {
            case "hour" -> localTime.getHour();
            case "minute" -> localTime.getMinute();
            case "second" -> localTime.getSecond();
            case "millisecond" -> localTime.getNano() / 1000000;
            case "microsecond" -> localTime.getNano() / 1000;
            case "nanosecond" -> localTime.getNano();
            default -> throw new RuntimeException("No such field: " + component);
        };
    }

    public String getComponentOfTimePoint(ZoneOffset zoneOffset, String component) {
        return switch (component) {
            case "timezone" -> zoneOffset.getId();
            case "offset" -> {
                String sign = "+";
                int second = zoneOffset.getTotalSeconds();
                if (second < 0) {
                    sign = "-";
                    second = -second;
                }
                yield sign + String.format("%02d%02d", second / 3600, (second % 3600) / 60);
            }
            case "offsetMinutes" -> String.valueOf(zoneOffset.getTotalSeconds() / 60);
            case "offsetSeconds" -> String.valueOf(zoneOffset.getTotalSeconds());
            default -> throw new RuntimeException("No such field: " + component);
        };
    }

    /**
     * @param object       对象节点/边/Map
     * @param propertyName 对象节点/边/Map的属性
     * @param timeWindow   值节点的有效时间限制
     * @return 在返回对象节点的某个属性的属性值时，如果在限制时间窗口内有多个属性值，返回一个列表；如果在限制时间窗口内有一个属性值，返回该值；如果在限制时间窗口内没有属性值，返回null。
     */
    @UserFunction("scypher.getPropertyValue")
    @Description("Get the property value of object node.")
    public Object getPropertyValue(@Name("object") Object object, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (object != null && propertyName != null) {
            if (object instanceof Node objectNode) {
                Node propertyNode = getPropertyNode(objectNode, propertyName);
                if (propertyNode != null) {
                    List<Node> valueNodeList = getValueNodes(propertyNode, timeWindow);
                    // 以开始时间的先后顺序排序值节点
                    valueNodeList.sort((o1, o2) -> {
                        STimePoint timePoint1 = new STimePoint(o1.getProperty("intervalFrom"));
                        STimePoint timePoint2 = new STimePoint(o2.getProperty("intervalTo"));
                        if (timePoint1.isBefore(timePoint2)) {
                            return -1;
                        } else if (timePoint1.isAfter(timePoint2)) {
                            return 1;
                        } else {
                            return 0;
                        }
                    });
                    List<Object> propertyValueList = new ArrayList<>();
                    for (Node valueNode : valueNodeList) {
                        propertyValueList.add(valueNode.getProperty("content"));
                    }
                    if (propertyValueList.size() == 0) {
                        return null;
                    }
                    if (propertyValueList.size() == 1) {
                        return propertyValueList.get(0);
                    }
                    return propertyValueList;
                } else {
                    return null;
                }
            } else if (object instanceof Relationship relationship) {
                if (relationship.hasProperty(propertyName)) {
                    return relationship.getProperty(propertyName);
                } else {
                    return null;
                }
            } else if (object instanceof Map) {
                Map<String, Object> objectMap = (Map<String, Object>) object;
                return objectMap.getOrDefault(propertyName, null);
            } else if (object instanceof TemporalAmount duration) {
                return switch (propertyName) {
                    case "years" -> duration.get(ChronoUnit.YEARS);
                    case "quarters" -> (int) duration.get(ChronoUnit.MONTHS) / 3;
                    case "months" -> duration.get(ChronoUnit.MONTHS);
                    case "weeks" -> duration.get(ChronoUnit.WEEKS);
                    case "days" -> duration.get(ChronoUnit.DAYS);
                    case "hours" -> duration.get(ChronoUnit.HOURS);
                    case "minutes" -> duration.get(ChronoUnit.MINUTES);
                    case "seconds" -> duration.get(ChronoUnit.SECONDS);
                    case "milliseconds" -> duration.get(ChronoUnit.MILLIS);
                    case "microseconds" -> duration.get(ChronoUnit.MICROS);
                    case "nanoseconds" -> duration.get(ChronoUnit.NANOS);
                    default -> throw new RuntimeException("No such field: " + propertyName);
                };
            } else if (object instanceof Date date) {
                return getComponentOfTimePoint(date, propertyName);
            } else if (object instanceof OffsetTime offsetTime) {
                if (propertyName.equals("timezone") | propertyName.equals("offset") | propertyName.equals("offsetMinutes") | propertyName.equals("offsetSeconds")) {
                    return getComponentOfTimePoint(offsetTime.getOffset(), propertyName);
                } else {
                    return getComponentOfTimePoint(offsetTime.toLocalTime(), propertyName);
                }
            } else if (object instanceof LocalTime localTime) {
                return getComponentOfTimePoint(localTime, propertyName);
            } else if (object instanceof LocalDateTime localDateTime) {
                if (propertyName.equals("hour") | propertyName.equals("minute") | propertyName.equals("second") | propertyName.equals("millisecond") |
                        propertyName.equals("microsecond") | propertyName.equals("nanosecond")) {
                    return getComponentOfTimePoint(localDateTime.toLocalTime(), propertyName);
                } else {
                    return getComponentOfTimePoint(Date.from(Timestamp.valueOf(localDateTime.toLocalDate().atStartOfDay()).toInstant()), propertyName);
                }
            } else if (object instanceof ZonedDateTime zonedDateTime) {
                if (propertyName.equals("hour") | propertyName.equals("minute") | propertyName.equals("second") | propertyName.equals("millisecond") |
                        propertyName.equals("microsecond") | propertyName.equals("nanosecond")) {
                    return getComponentOfTimePoint(zonedDateTime.toLocalTime(), propertyName);
                } else if (propertyName.equals("timezone") | propertyName.equals("offset") | propertyName.equals("offsetMinutes") | propertyName.equals("offsetSeconds")) {
                    return getComponentOfTimePoint(zonedDateTime.getOffset(), propertyName);
                } else {
                    return getComponentOfTimePoint(Date.from(Timestamp.valueOf(zonedDateTime.toLocalDate().atStartOfDay()).toInstant()), propertyName);
                }
            } else if (object instanceof Point point) {
                if (propertyName.equals("x") | propertyName.equals("longitude")) {
                    return point.getCoordinate().getCoordinate()[0];
                } else if (propertyName.equals("y") | propertyName.equals("latitude")) {
                    if (point.getCoordinate().getCoordinate().length > 1) {
                        return point.getCoordinates().get(1);
                    } else {
                        throw new RuntimeException(propertyName + " is not available on point");
                    }
                } else if (propertyName.equals("z") | propertyName.equals("height")) {
                    if (point.getCoordinate().getCoordinate().length > 2) {
                        return point.getCoordinates().get(2);
                    } else {
                        throw new RuntimeException(propertyName + " is not available on point");
                    }
                } else if (propertyName.equals("crs")) {
                    return point.getCRS();
                } else if (propertyName.equals("srid")) {
                    return point.getCRS().getCode();
                } else {
                    throw new RuntimeException("No such field: " + propertyName);
                }
            } else {
                throw new RuntimeException("Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was " + object.getClass().getSimpleName());
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param object 对象节点/边
     * @return 返回对象节点/边的有效时间
     */
    @UserFunction("scypher.getObjectEffectiveTime")
    @Description("Get the effective time of object node or relationship.")
    public Map<String, Object> getObjectEffectiveTime(@Name("node") Object object) {
        if (object != null) {
            if (object instanceof Node objectNode) {
                return new SInterval(new STimePoint(objectNode.getProperty("intervalFrom")), new STimePoint(objectNode.getProperty("intervalTo"))).getSystemInterval();
            } else if (object instanceof Relationship relationship) {
                return new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo"))).getSystemInterval();
            } else {
                throw new RuntimeException("Type mismatch: expected Node or Relationship but was " + object.getClass().getSimpleName());
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param object       对象节点/Map类型数据
     * @param propertyName 属性名
     * @return 如果objectNode为对象节点，返回对应属性节点的有效时间；如果objectNode为Map类型数据，且objectNode.propertyName为对象节点/边，返回该对象节点/边的有效时间。
     */
    @UserFunction("scypher.getPropertyEffectiveTime")
    @Description("Get the effective time of property node.")
    public Object getPropertyEffectiveTime(@Name("node") Object object, @Name("propertyName") String propertyName) {
        if (object != null && propertyName != null) {
            if (object instanceof Node objectNode) {
                Node propertyNode = getPropertyNode(objectNode, propertyName);
                return new SInterval(new STimePoint(propertyNode.getProperty("intervalFrom")), new STimePoint(propertyNode.getProperty("intervalTo"))).getSystemInterval();
            } else if (object instanceof Map) {
                Map<String, Object> objectMap = (Map<String, Object>) object;
                if (objectMap.containsKey(propertyName)) {
                    if (objectMap.get(propertyName) instanceof Node objectNode) {
                        return new SInterval(new STimePoint(objectNode.getProperty("intervalFrom")), new STimePoint(objectNode.getProperty("intervalTo"))).getSystemInterval();
                    } else if (objectMap.get(propertyName) instanceof Relationship relationship) {
                        return new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo"))).getSystemInterval();
                    } else {
                        throw new RuntimeException(objectMap.get(propertyName).getClass().getSimpleName() + " doesn't have effective time");
                    }
                } else {
                    throw new RuntimeException("NULL doesn't have effective time");
                }
            } else {
                throw new RuntimeException("Type mismatch: expected Node or Map but was " + object.getClass().getSimpleName());
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param objectNode   对象节点
     * @param propertyName 属性名
     * @param timeWindow   值节点的有效时间限制
     * @return 返回对象节点的某个属性下的满足有效时间限制的所有值节点
     */
    @UserFunction("scypher.getValueEffectiveTime")
    @Description("Get the effective time of value node.")
    public Object getValueEffectiveTime(@Name("node") Node objectNode, @Name("propertyName") String propertyName, @Name("timeWindow") Object timeWindow) {
        if (objectNode != null && propertyName != null) {
            Node propertyNode = getPropertyNode(objectNode, propertyName);
            if (propertyNode != null) {
                List<Node> valueNodeList = getValueNodes(propertyNode, timeWindow);
                // 以开始时间的先后顺序排序值节点
                valueNodeList.sort((o1, o2) -> {
                    STimePoint timePoint1 = new STimePoint(o1.getProperty("intervalFrom"));
                    STimePoint timePoint2 = new STimePoint(o2.getProperty("intervalTo"));
                    if (timePoint1.isBefore(timePoint2)) {
                        return -1;
                    } else if (timePoint1.isAfter(timePoint2)) {
                        return 1;
                    } else {
                        return 0;
                    }
                });
                List<Map<String, Object>> valueEffectiveTimeList = new ArrayList<>();
                for (Node valueNode : valueNodeList) {
                    valueEffectiveTimeList.add(new SInterval(new STimePoint(valueNode.getProperty("intervalFrom")), new STimePoint(valueNode.getProperty("intervalTo"))).getSystemInterval());
                }
                if (valueEffectiveTimeList.size() == 0) {
                    return null;
                }
                if (valueEffectiveTimeList.size() == 1) {
                    return valueEffectiveTimeList.get(0);
                }
                return valueEffectiveTimeList;
            } else {
                return null;
            }
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}

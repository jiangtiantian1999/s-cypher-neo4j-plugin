package cn.scypher.neo4j.plugin;

import cn.scypher.neo4j.plugin.datetime.SInterval;
import cn.scypher.neo4j.plugin.datetime.STimePoint;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.impl.ExtendedPath;
import org.neo4j.procedure.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TemporalPathQuery {

    public static class TemporalPath {
        public Path path;

        public TemporalPath(Path path) {
            this.path = path;
        }
    }

    public interface ContinuousPathAlgo {
        boolean check(SInterval interval, SInterval otherInterval);

        SInterval update(SInterval interval, SInterval otherInterval);
    }

    public List<TemporalPath> getDirectedContinuousPath(Node startNode, Node endNode, List<String> labels, Long minLength, Long maxLength,
                                                        STimePoint relationshipTimePoint, SInterval relationshipInterval, Map<String, Object> properties, ContinuousPathAlgo continuousPathAlgo) {
        List<TemporalPath> continuousPaths = new ArrayList<>();
        String timePointType = GlobalVariablesManager.getTimePointType();
        String timezone = GlobalVariablesManager.getTimezone();
        SInterval initialInterval = new SInterval(STimePoint.min(timePointType, timezone), STimePoint.max(timePointType, timezone));
        List<Path> pathQueue = new ArrayList<>();
        pathQueue.add(PathImpl.singular(startNode));
        List<SInterval> pathIntervals = new ArrayList<>();
        pathIntervals.add(initialInterval);
        while (pathQueue.size() > 0) {
            Path currentPath = pathQueue.remove(pathQueue.size() - 1);
            List<String> nodes_id = new ArrayList<>();
            currentPath.nodes().forEach(node -> nodes_id.add(node.getElementId()));
            SInterval pathInterval = pathIntervals.remove(pathIntervals.size() - 1);
            Node currentNode = currentPath.endNode();
            // 限制边的方向、标签、有效时间和属性
            List<Relationship> relationships = currentNode.getRelationships(Direction.OUTGOING).stream().filter(relationship -> {
                String type = relationship.getType().name();
                if (!type.equals("OBJECT_PROPERTY") && !type.equals("PROPERTY_VALUE")) {
                    return labels.size() == 0 | labels.contains(type);
                }
                return false;
            }).filter(relationship -> {
                SInterval relationshipEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                if (relationshipInterval != null) {
                    return relationshipEffectiveTime.overlaps(relationshipInterval);
                } else if (relationshipTimePoint != null) {
                    return relationshipEffectiveTime.contains(relationshipTimePoint);
                }
                return true;
            }).filter(relationship -> {
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    if (!relationship.hasProperty(entry.getKey())) {
                        return false;
                    } else if (!relationship.getProperty(entry.getKey()).equals(entry.getValue())) {
                        return false;
                    }
                }
                return true;
            }).toList();
            for (Relationship relationship : relationships) {
                // 避免陷入死循环
                if (!nodes_id.contains(relationship.getEndNode().getElementId())) {
                    SInterval relationshipEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                    // 判断路径是否满足时序条件
                    if (continuousPathAlgo.check(pathInterval, relationshipEffectiveTime)) {
                        Path path = new ExtendedPath(currentPath, relationship);
                        if (path.length() >= minLength && path.length() <= maxLength) {
                            if (relationship.getEndNode().equals(endNode)) {
                                continuousPaths.add(new TemporalPath(path));
                            }
                        }
                        if (path.length() < maxLength) {
                            pathQueue.add(path);
                            pathIntervals.add(continuousPathAlgo.update(pathInterval, relationshipEffectiveTime));
                        }
                    }
                }
            }
        }
        return continuousPaths;
    }

    public List<TemporalPath> getDirectedSequentialPath(Node startNode, Node endNode, List<String> labels, Long minLength, Long maxLength,
                                                        STimePoint relationshipTimePoint, SInterval relationshipInterval, Map<String, Object> properties, String sPathType) {
        List<TemporalPath> sequentialPaths = new ArrayList<>();
        List<Path> pathQueue = new ArrayList<>();
        pathQueue.add(PathImpl.singular(startNode));
        // 求最早到达路径和最迟出发路径
        String timePointType = GlobalVariablesManager.getTimePointType();
        String timezone = GlobalVariablesManager.getTimezone();
        STimePoint timePoint = null;
        if (sPathType.equals("E")) {
            timePoint = STimePoint.max(timePointType, timezone);
        } else if (sPathType.equals("L")) {
            timePoint = STimePoint.min(timePointType, timezone);
        }
        // 求最短路径
        long length = Long.MAX_VALUE;
        // 求最快路径
        Duration duration = Duration.between(LocalDateTime.MIN, LocalDateTime.MAX);
        while (pathQueue.size() > 0) {
            Path currentPath = pathQueue.remove(pathQueue.size() - 1);
            Node currentNode = currentPath.endNode();
            List<String> nodes_id = new ArrayList<>();
            currentPath.nodes().forEach(node -> nodes_id.add(node.getElementId()));
            // 限制边的方向、标签、有效时间和属性
            List<Relationship> relationships = currentNode.getRelationships(Direction.OUTGOING).stream().filter(relationship -> {
                String type = relationship.getType().name();
                if (!type.equals("OBJECT_PROPERTY") && !type.equals("PROPERTY_VALUE")) {
                    return labels.size() == 0 | labels.contains(type);
                }
                return false;
            }).filter(relationship -> {
                SInterval relationshipEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                if (relationshipInterval != null) {
                    return relationshipEffectiveTime.overlaps(relationshipInterval);
                } else if (relationshipTimePoint != null) {
                    return relationshipEffectiveTime.contains(relationshipTimePoint);
                }
                return true;
            }).filter(relationship -> {
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    if (!relationship.hasProperty(entry.getKey())) {
                        return false;
                    } else if (!relationship.getProperty(entry.getKey()).equals(entry.getValue())) {
                        return false;
                    }
                }
                return true;
            }).toList();
            Relationship lastRelationship = currentPath.lastRelationship();
            for (Relationship relationship : relationships) {
                // 避免重复访问节点
                if (!nodes_id.contains(relationship.getEndNode().getElementId())) {
                    SInterval relationshipEffectiveTime = new SInterval(new STimePoint(relationship.getProperty("intervalFrom")), new STimePoint(relationship.getProperty("intervalTo")));
                    // 判断路径是否为顺序有效路径
                    if (lastRelationship == null | (lastRelationship != null && new STimePoint(lastRelationship.getProperty("intervalTo")).isNotAfter(relationshipEffectiveTime.getIntervalFrom()))) {
                        Path path = new ExtendedPath(currentPath, relationship);
                        if (path.length() >= minLength && path.length() <= maxLength) {
                            if (relationship.getEndNode().equals(endNode)) {
                                switch (sPathType) {
                                    case "E" -> {
                                        if (relationshipEffectiveTime.getIntervalTo().isNotAfter(timePoint)) {
                                            if (relationshipEffectiveTime.getIntervalTo().isBefore(timePoint)) {
                                                sequentialPaths.clear();
                                                timePoint = relationshipEffectiveTime.getIntervalTo();
                                            }
                                            sequentialPaths.add(new TemporalPath(path));
                                        }
                                    }
                                    case "L" -> {
                                        Relationship firstRelationship = path.relationships().iterator().next();
                                        STimePoint firstRelationshipStartTime = new STimePoint(firstRelationship.getProperty("intervalFrom"));
                                        if (firstRelationshipStartTime.isNotBefore(timePoint)) {
                                            if (firstRelationshipStartTime.isAfter(timePoint)) {
                                                sequentialPaths.clear();
                                                timePoint = firstRelationshipStartTime;
                                            }
                                            sequentialPaths.add(new TemporalPath(path));
                                        }
                                    }
                                    case "S" -> {
                                        if (path.length() <= length) {
                                            if (path.length() < length) {
                                                sequentialPaths.clear();
                                                length = path.length();
                                            }
                                            sequentialPaths.add(new TemporalPath(path));
                                        }
                                    }
                                    case "F" -> {
                                        Relationship firstRelationship = path.relationships().iterator().next();
                                        STimePoint firstRelationshipStartTime = new STimePoint(firstRelationship.getProperty("intervalFrom"));
                                        Duration currentDuration = firstRelationshipStartTime.difference(relationshipEffectiveTime.getIntervalTo());
                                        if (currentDuration.compareTo(duration) <= 0) {
                                            if (currentDuration.compareTo(duration) < 0) {
                                                sequentialPaths.clear();
                                                duration = currentDuration;
                                            }
                                            sequentialPaths.add(new TemporalPath(path));
                                        }
                                    }
                                }
                            }
                        }
                        if (path.length() < maxLength) {
                            switch (sPathType) {
                                case "E" -> {
                                    if (relationshipEffectiveTime.getIntervalTo().isNotAfter(timePoint)) {
                                        pathQueue.add(path);
                                    }
                                }
                                case "L" -> {
                                    Relationship firstRelationship = path.relationships().iterator().next();
                                    STimePoint firstRelationshipStartTime = new STimePoint(firstRelationship.getProperty("intervalFrom"));
                                    if (firstRelationshipStartTime.isNotBefore(timePoint)) {
                                        pathQueue.add(path);
                                    }
                                }
                                case "S" -> {
                                    if (path.length() <= length) {
                                        pathQueue.add(path);
                                    }
                                }
                                case "F" -> {
                                    Relationship firstRelationship = path.relationships().iterator().next();
                                    STimePoint firstRelationshipStartTime = new STimePoint(firstRelationship.getProperty("intervalFrom"));
                                    Duration currentDuration = firstRelationshipStartTime.difference(relationshipEffectiveTime.getIntervalTo());
                                    if (currentDuration.compareTo(duration) <= 0) {
                                        pathQueue.add(path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return sequentialPaths;
    }

    /**
     * 读取时序路径的信息
     *
     * @param startNode          开始节点
     * @param endNode            结束节点
     * @param isUndirected       是否有方向
     * @param pathInfo           路径信息
     * @param continuousPathAlgo 连续路径算法
     * @param sPathType          顺序路径类型
     * @return 返回满足条件的时序路径
     */
    public List<TemporalPath> getTemporalPath(Node startNode, Node endNode, boolean isUndirected, Map<String, Object> pathInfo, ContinuousPathAlgo continuousPathAlgo, String sPathType) {
        // 读取路径的限制信息
        List<String> labels;
        long minLength = 1L;
        long maxLength = Long.MAX_VALUE;
        STimePoint relationshipTimePoint;
        SInterval relationshipInterval;
        Map<String, Object> properties;
        if (pathInfo != null && pathInfo.containsKey("labels")) {
            labels = (List<String>) pathInfo.get("labels");
        } else {
            labels = new ArrayList<>();
        }
        if (pathInfo != null && pathInfo.containsKey("minLength")) {
            minLength = (Long) pathInfo.get("minLength");
        }
        if (pathInfo != null && pathInfo.containsKey("maxLength")) {
            maxLength = (Long) pathInfo.get("maxLength");
        }
        if (pathInfo != null && pathInfo.containsKey("effectiveTime")) {
            if (pathInfo.get("effectiveTime") instanceof Map) {
                relationshipTimePoint = null;
                relationshipInterval = new SInterval((Map<String, Object>) pathInfo.get("effectiveTime"));
            } else {
                relationshipInterval = null;
                relationshipTimePoint = new STimePoint(pathInfo.get("effectiveTime"));
            }
        } else {
            STimePoint snapshotTimePoint = GlobalVariablesManager.getSnapshotTimePoint();
            SInterval scopeInterval = GlobalVariablesManager.getScopeInterval();
            if (scopeInterval != null) {
                relationshipTimePoint = null;
                relationshipInterval = scopeInterval;
            } else if (snapshotTimePoint != null) {
                relationshipInterval = null;
                relationshipTimePoint = snapshotTimePoint;
            } else {
                relationshipTimePoint = null;
                relationshipInterval = null;
            }
        }
        if (pathInfo != null && pathInfo.containsKey("properties")) {
            properties = (Map<String, Object>) pathInfo.get("properties");
        } else {
            properties = new HashMap<>();
        }
        List<TemporalPath> temporalPaths = null;
        if (continuousPathAlgo != null) {
            temporalPaths = getDirectedContinuousPath(startNode, endNode, labels, minLength, maxLength, relationshipTimePoint, relationshipInterval, properties, continuousPathAlgo);
        } else if (sPathType != null) {
            temporalPaths = getDirectedSequentialPath(startNode, endNode, labels, minLength, maxLength, relationshipTimePoint, relationshipInterval, properties, sPathType);
        }
        if (isUndirected) {
            if (continuousPathAlgo != null) {
                temporalPaths.addAll(getDirectedContinuousPath(endNode, startNode, labels, minLength, maxLength, relationshipTimePoint, relationshipInterval, properties, continuousPathAlgo));
            } else if (sPathType != null) {
                temporalPaths.addAll(getDirectedSequentialPath(endNode, startNode, labels, minLength, maxLength, relationshipTimePoint, relationshipInterval, properties, sPathType));
            }
        }
        return temporalPaths;
    }

    /**
     * @param startNode    开始节点
     * @param endNode      结束节点
     * @param isUndirected 路径的方向是否为无向
     * @param pathInfo     路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength、最大长度maxLength、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回连续有效路径
     */
    @Procedure(name = "scypher.cPath", mode = Mode.READ)
    @Description("Find the continuous paths meets the requirements.")
    public Stream<TemporalPath> cPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("isUndirected") boolean isUndirected, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            return getTemporalPath(startNode, endNode, isUndirected, pathInfo, new ContinuousPathAlgo() {
                @Override
                public boolean check(SInterval interval, SInterval otherInterval) {
                    return interval.overlaps(otherInterval);
                }

                @Override
                public SInterval update(SInterval interval, SInterval otherInterval) {
                    return interval.intersection(otherInterval);
                }
            }, null).stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode    开始节点
     * @param endNode      结束节点
     * @param isUndirected 路径的方向是否为无向
     * @param pathInfo     路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength、最大长度maxLength、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回成对连续有效路径
     */
    @Procedure(name = "scypher.pairCPath", mode = Mode.READ)
    @Description("Find the pairwise continuous paths meets the requirements.")
    public Stream<TemporalPath> pairCPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("isUndirected") boolean isUndirected, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            return getTemporalPath(startNode, endNode, isUndirected, pathInfo, new ContinuousPathAlgo() {
                @Override
                public boolean check(SInterval interval, SInterval otherInterval) {
                    return interval.overlaps(otherInterval);
                }

                @Override
                public SInterval update(SInterval interval, SInterval otherInterval) {
                    return otherInterval;
                }
            }, null).stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode    开始节点
     * @param endNode      结束节点
     * @param isUndirected 路径的方向是否为无向
     * @param pathInfo     路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength、最大长度maxLength、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最早顺序有效路径
     */
    @Procedure(name = "scypher.earliestSPath", mode = Mode.READ)
    @Description("Find the earliest sequential paths meets the requirements.")
    public Stream<TemporalPath> earliestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("isUndirected") boolean isUndirected, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            return getTemporalPath(startNode, endNode, isUndirected, pathInfo, null, "E").stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode    开始节点
     * @param endNode      结束节点
     * @param isUndirected 路径的方向是否为无向
     * @param pathInfo     路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength、最大长度maxLength、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最迟顺序有效路径
     */
    @Procedure(name = "scypher.latestSPath", mode = Mode.READ)
    @Description("Find the latest sequential paths meets the requirements.")
    public Stream<TemporalPath> latestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("isUndirected") boolean isUndirected, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            return getTemporalPath(startNode, endNode, isUndirected, pathInfo, null, "L").stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode    开始节点
     * @param endNode      结束节点
     * @param isUndirected 路径的方向是否为无向
     * @param pathInfo     路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength、最大长度maxLength、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最快顺序有效路径
     */
    @Procedure(name = "scypher.fastestSPath", mode = Mode.READ)
    @Description("Find the fastest sequential paths meets the requirements.")
    public Stream<TemporalPath> fastestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("isUndirected") boolean isUndirected, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            return getTemporalPath(startNode, endNode, isUndirected, pathInfo, null, "F").stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode    开始节点
     * @param endNode      结束节点
     * @param isUndirected 路径的方向是否为无向
     * @param pathInfo     路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength、最大长度maxLength、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最短顺序有效路径
     */
    @Procedure(name = "scypher.shortestSPath", mode = Mode.READ)
    @Description("Find the shortest sequential paths meets the requirements.")
    public Stream<TemporalPath> shortestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("isUndirected") boolean isUndirected, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            return getTemporalPath(startNode, endNode, isUndirected, pathInfo, null, "S").stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}

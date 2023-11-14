package cn.scypher.neo4j.plugin;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TemporalPathQuery {

    public static class TemporalPath {
        public Path path;

        public TemporalPath(Path path) {
            this.path = path;
        }
    }

    /**
     * @param startNode 开始节点
     * @param endNode   结束节点
     * @param pathInfo  路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength(Integer)、最大长度maxLength(Integer)、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回连续有效路径
     */
    @Procedure(name = "scypher.cPath", mode = Mode.READ)
    @Description("Find the continuous paths meets the requirements.")
    public Stream<TemporalPath> cPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            List<TemporalPath> continuousPathList = new ArrayList<>();
            //TODO
            return continuousPathList.stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode 开始节点
     * @param endNode   结束节点
     * @param pathInfo  路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength(Integer)、最大长度maxLength(Integer)、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回成对连续有效路径
     */
    @Procedure(name = "scypher.pairCPath", mode = Mode.READ)
    @Description("Find the pairwise continuous paths meets the requirements.")
    public Stream<TemporalPath> pairCPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            List<TemporalPath> pairwiseCPathList = new ArrayList<>();
            //TODO
            return pairwiseCPathList.stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode 开始节点
     * @param endNode   结束节点
     * @param pathInfo  路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength(Integer)、最大长度maxLength(Integer)、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最早顺序有效路径
     */
    @Procedure(name = "scypher.earliestSPath", mode = Mode.READ)
    @Description("Find the earliest sequential paths meets the requirements.")
    public Stream<TemporalPath> earliestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            List<TemporalPath> earliestSPathList = new ArrayList<>();
            //TODO
            return earliestSPathList.stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode 开始节点
     * @param endNode   结束节点
     * @param pathInfo  路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength(Integer)、最大长度maxLength(Integer)、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最迟顺序有效路径
     */
    @Procedure(name = "scypher.latestSPath", mode = Mode.READ)
    @Description("Find the latest sequential paths meets the requirements.")
    public Stream<TemporalPath> latestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            List<TemporalPath> latestSPathList = new ArrayList<>();
            //TODO
            return latestSPathList.stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode 开始节点
     * @param endNode   结束节点
     * @param pathInfo  路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength(Integer)、最大长度maxLength(Integer)、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最快顺序有效路径
     */
    @Procedure(name = "scypher.fastestSPath", mode = Mode.READ)
    @Description("Find the fastest sequential paths meets the requirements.")
    public Stream<TemporalPath> fastestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            List<TemporalPath> fastestSPathList = new ArrayList<>();
            //TODO
            return fastestSPathList.stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }

    /**
     * @param startNode 开始节点
     * @param endNode   结束节点
     * @param pathInfo  路径的限制条件，为Map类型，可能包括路径的标签labels(List)、最小长度minLength(Integer)、最大长度maxLength(Integer)、路径的有效时间effectiveTime(时间点/时间区间)、路径的属性properties(Map<String, Object>)
     * @return 返回最短顺序有效路径
     */
    @Procedure(name = "scypher.shortestSPath", mode = Mode.READ)
    @Description("Find the shortest sequential paths meets the requirements.")
    public Stream<TemporalPath> shortestSPath(@Name("startNode") Node startNode, @Name("endNode") Node endNode, @Name("pathInfo") Map<String, Object> pathInfo) {
        if (startNode != null && endNode != null) {
            List<TemporalPath> shortestSPathList = new ArrayList<>();
            //TODO
            return shortestSPathList.stream();
        } else {
            throw new RuntimeException("Missing parameter");
        }
    }
}

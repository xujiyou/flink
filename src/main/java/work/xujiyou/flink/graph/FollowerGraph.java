package work.xujiyou.flink.graph;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FollowerGraph class
 *
 * @author jiyouxu
 * @date 2019/12/20
 */
public class FollowerGraph {

    static ElasticSearchDataSource elasticSearchDataSource = new ElasticSearchDataSource();

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final List<Vertex<String, Map<String, Object>>> vertexList = new ArrayList<>();
        final List<Edge<String, String>> edgeList = new ArrayList<>();
        final List<Map<String, Object>> dataList = elasticSearchDataSource.findDataList();

        Map<String, Object> oneMap = new HashMap<>();
        oneMap.put("url_token", "excited-vczh");
        oneMap.put("name", "vczh");
        oneMap.put("headline", "专业造轮子，拉黑抢前排。gaclib.net");
        dataList.add(0, oneMap);

        dataList.forEach((follower) -> {
            Vertex<String, Map<String, Object>> vertex = new Vertex<>(follower.get("url_token").toString(), follower);
            vertexList.add(vertex);
            if (follower.get("v") != null) {
                Edge<String, String> edge = new Edge<>(follower.get("url_token").toString(), follower.get("v").toString(), "1");
                edgeList.add(edge);
            }
        });

        DataSet<Vertex<String, Map<String, Object>>> vertices = env.fromCollection(vertexList);
        DataSet<Edge<String, String>> edges = env.fromCollection(edgeList);
        Graph<String, Map<String, Object>, String> graph = Graph.fromDataSet(vertices, edges, env);
        System.out.println(graph.getVertices().collect().size());

        Graph<String, Map<String, Object>, String> result = graph.runVertexCentricIteration(new SSSPComputeFunction(), new SSSPCombiner(), 10000);
        System.out.println(result.getVertices().collect().size());
    }

    public static final class SSSPComputeFunction extends ComputeFunction<String, Map<String, Object>, String, String> {
        @Override
        public void compute(Vertex<String, Map<String, Object>> vertex, MessageIterator<String> strings) {
            System.out.println(vertex.getValue().get("name") + ":" + vertex.getValue().get("headline"));
        }
    }

    public static final class SSSPCombiner extends MessageCombiner<String, String> {
        @Override
        public void combineMessages(MessageIterator<String> strings) {
            System.out.println(strings);
        }
    }
}


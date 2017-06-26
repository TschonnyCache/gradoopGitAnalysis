package analysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.split.Split;
import org.gradoop.flink.util.GradoopFlinkConfig;

import gradoopify.GradoopFiller;

public class GitAnalyzer implements Serializable {

	private static final long serialVersionUID = 5400004312044745133L;

	public static final String branchGraphHeadLabel = "branch";
	public static final String latestCommitHashLabel = "latestCommitHash";

	public LogicalGraph createUserCount(LogicalGraph graph) {
		LogicalGraph userSubGraph = graph.subgraph(new FilterFunction<Vertex>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -1733961439448245556L;

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.userVertexLabel);
			}

		}, new FilterFunction<Edge>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1679554295734115110L;

			@Override
			public boolean filter(Edge arg0) throws Exception {
				return false;
			}

		});
		return userSubGraph.aggregate(new VertexCount());
	}

	/**
	 * Adds Subgraphs to the graph which represent a branch. each of these
	 * branch subgraphs then contains all the commits belonging to the branch
	 * and the corresponding edges between the commits and the branch vertices.
	 */
	public GraphCollection transformBranchesToSubgraphs(LogicalGraph graph, GradoopFlinkConfig config)
			throws Exception {
		LogicalGraph onlyBranchVerticesGraph = graph.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.branchVertexLabel);
			}

		});
		List<Vertex> allBranches = onlyBranchVerticesGraph.getVertices().collect();
		List<Edge> allEdges = graph.getEdges().collect();
		List<LogicalGraph> resGraphs = new ArrayList<LogicalGraph>();
		for (Vertex branch : allBranches) {
			LogicalGraph currentBranchSubGraph = graph.subgraph(new FilterFunction<Vertex>() {

				// Checks if the is an edge between the current vertex and
				// current branch vertex
				@Override
				public boolean filter(Vertex v) throws Exception {
					for (Edge edge : allEdges) {
						if (edge.getSourceId().equals(v.getId()) && edge.getTargetId().equals(branch.getId())) {
							return true;
						}
					}
					return false;
				}

			}, new FilterFunction<Edge>() {

				@Override
				public boolean filter(Edge e) throws Exception {
					System.out.println(branch.getId() + " == "+ e.getTargetId());
					if (e.getTargetId().equals(branch.getId())) {
						return true;
					}
					return false;
				}

			});
			currentBranchSubGraph = setBranchLabelAsGraphHeadProperty(currentBranchSubGraph, branch.getPropertyValue(GradoopFiller.branchVertexFieldName).getString());
			currentBranchSubGraph = addLatestCommitOnThisBranchAsProperty(currentBranchSubGraph);
			resGraphs.add(currentBranchSubGraph);
		}
		if(resGraphs.size() == 0){
			return null;
		}
        DataSet<Vertex> tmpVertices = resGraphs.get(0).getVertices();
        DataSet<Edge> tmpEdges = resGraphs.get(0).getEdges();
        List<GraphHead> tmpGraphHeads = new ArrayList<GraphHead>();
        tmpGraphHeads.add(resGraphs.get(0).getGraphHead().collect().get(0));

        //Fix bug that vertices/edges with same Ids have different GradoopIdLists by joining them
        for(int i = 1; i < resGraphs.size(); i++){
        	LogicalGraph g = resGraphs.get(i);
        	tmpVertices = tmpVertices
                .join(g.getVertices())
                .where(new Id<Vertex>())
                .equalTo(new Id<Vertex>())
                .with(new VertexJoiner());

            tmpEdges = tmpEdges
                .union(g.getEdges());
        	
        	tmpGraphHeads.add(g.getGraphHead().collect().get(0));
        }
        List<Vertex> fV = tmpVertices.collect();
        List<Edge> fE = tmpEdges.collect();
        GraphCollection result = GraphCollection.fromDataSets(config.getExecutionEnvironment().fromCollection(tmpGraphHeads), tmpVertices, tmpEdges, config);
		return result;
	}
	
	public class VertexJoiner
    implements JoinFunction<Vertex, Vertex, Vertex>{

		@Override
		public Vertex join(Vertex first, Vertex second) throws Exception {
			// TODO Auto-generated method stub
            GradoopIdList firstIdList = first.getGraphIds();
            for(GradoopId id: second.getGraphIds()){
            	if(!firstIdList.contains(id)){
            		firstIdList.add(id);
            	}
            }
            first.setGraphIds(firstIdList);
            return first;
		}
    }

    private List<PropertyValue> getSplitValues(Vertex v) {
      String key = "name";
      List<PropertyValue> valueList = new ArrayList<>();
      if (v.hasProperty(key)) {
        valueList.add(v.getPropertyValue(key));
      }
      return valueList;
    }
	
	public LogicalGraph setBranchLabelAsGraphHeadProperty(LogicalGraph g, String branchName){
			LogicalGraph result = g.transformGraphHead(new TransformationFunction<GraphHead>() {

				@Override
				public GraphHead apply(GraphHead current, GraphHead transformed) {
					current.setLabel(branchGraphHeadLabel);
					current.setProperty("name", branchName);
					return current;
				}

			});
		return result;
	}

	public LogicalGraph getGraphFromCollectionByBranchName(GraphCollection gc, String branchName) throws Exception {
		List<GraphHead> graphHead = gc.select(new FilterFunction<GraphHead>() {
			@Override
			public boolean filter(GraphHead entity) throws Exception {
				boolean correct = entity.hasProperty("name")
						&& entity.getPropertyValue("name").getString().equals(branchName);
				return correct;
			}
		}).getGraphHeads().collect();
		if (graphHead == null || graphHead.size() == 0 || graphHead.size() > 1) {
			System.err.println("Too many or no graph heads found! Returning null");
			return null;
		}
		GradoopId graphID = graphHead.get(0).getId();
		DataSet<Vertex> vertices = gc.getVertices().filter(new InGraph<>(graphID));
		DataSet<Edge> edges = gc.getEdges().filter(new InGraph<>(graphID));
		List<Vertex> test1 = vertices.collect();
		List<Edge> test2 = edges.collect();
		LogicalGraph result = LogicalGraph.fromDataSets(gc.getConfig().getExecutionEnvironment().fromElements(graphHead.get(0)), vertices, edges, gc.getConfig());
		return result;
	}

	public LogicalGraph addLatestCommitOnThisBranchAsProperty(LogicalGraph g) throws Exception {
		MinVertexProperty mvp = new MinVertexProperty("time");
		LogicalGraph withMinTime = g.aggregate(mvp);
		int minTime = withMinTime.getGraphHead().collect().get(0).getPropertyValue(mvp.getAggregatePropertyKey())
				.getInt();
		LogicalGraph filtered = g.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(GradoopFiller.commitVertexLabel)) {
					if (v.getPropertyValue("time").getInt() == minTime) {
						return true;
					}
				}
				return false;
			}
		});
		String latestCommitHash = filtered.getVertices().collect().get(0).getPropertyValue("name").getString();
		LogicalGraph res = g.transformGraphHead(new TransformationFunction<GraphHead>() {

			@Override
			public GraphHead apply(GraphHead current, GraphHead transformed) {
				// current.setLabel(current.getLabel());
				// current.setProperties(current.getProperties());
				current.setProperty(latestCommitHashLabel, latestCommitHash);
				return current;
			}

		});
		return res;
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(".");
		GitAnalyzer ga = new GitAnalyzer();
		// LogicalGraph userCountGraph = ga.createUserCount(graph);
		// userCountGraph.getGraphHead().print();
		GraphCollection branchGroupedGraph = ga.transformBranchesToSubgraphs(graph, gradoopConf);
		DataSink jsonSink = new JSONDataSink("./out", gradoopConf);
		branchGroupedGraph.writeTo(jsonSink);
		env.execute();
	}
}

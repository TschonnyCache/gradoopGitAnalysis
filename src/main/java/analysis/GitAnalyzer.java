package analysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
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
		LogicalGraph onlyBranchVerticesGraph = graph.subgraph(new FilterFunction<Vertex>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -6111079302462324343L;

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.branchVertexLabel);
			}

		}, new FilterFunction<Edge>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -4095888214789879745L;

			@Override
			public boolean filter(Edge arg0) throws Exception {
				return false;
			}

		});
		List<Vertex> allBranches = onlyBranchVerticesGraph.getVertices().collect();
		List<Edge> allEdges = graph.getEdges().collect();
		List<GraphHead> branchSubgraphsHeads = new ArrayList<GraphHead>();
		for (Vertex branch : allBranches) {
			LogicalGraph currentBranchSubGraph = graph.subgraph(new FilterFunction<Vertex>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = -2871983887699356318L;

				// Checks if the is an edge between the current vertex and
				// current branch vertex
				@Override
				public boolean filter(Vertex v) throws Exception {
					for (Edge edge : allEdges) {
						System.out.println(v.getId() + "   ->   " + branch.getId());
						if (edge.getSourceId().equals(v.getId()) && edge.getTargetId().equals(branch.getId())) {
							return true;
						}
					}
					return false;
				}

			}, new FilterFunction<Edge>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = -1480588024727139895L;

				@Override
				public boolean filter(Edge e) throws Exception {
					if (e.getTargetId().equals(branch.getId())) {
						return true;
					}
					return false;
				}

			});
			System.out.println("Label before: " + currentBranchSubGraph.getGraphHead().collect().get(0).getLabel());
			currentBranchSubGraph = currentBranchSubGraph.transformGraphHead(new TransformationFunction<GraphHead>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = -5530256569602123586L;

				@Override
				public GraphHead apply(GraphHead current, GraphHead transformed) {
					transformed.setLabel(branchGraphHeadLabel);
					transformed.setProperty("name", branch.getPropertyValue("name").getString());
					return transformed;
				}

			});
			currentBranchSubGraph = addLatestCommitOnThisBranchAsProperty(currentBranchSubGraph);
			System.out.println("Label after: " + currentBranchSubGraph.getGraphHead().collect().get(0));
			branchSubgraphsHeads.add(currentBranchSubGraph.getGraphHead().collect().get(0));

		}
		return GraphCollection.fromCollections(branchSubgraphsHeads, graph.getVertices().collect(),
				graph.getEdges().collect(), config);
	}
	
	public LogicalGraph getGraphFromCollectionByBranchName(GraphCollection gc, String branchName) throws Exception{
		List<GraphHead> graphHead = gc.select(new FilterFunction<GraphHead>() {
				      @Override
				      public boolean filter(GraphHead entity) throws Exception {
				        return entity.hasProperty("name") &&
				          entity.getPropertyValue("name").getString().equals(branchName);
				      }
				    }).getGraphHeads().collect();
		if(graphHead == null || graphHead.size() == 0 || graphHead.size() > 1){
			System.err.println("Too many or no graph heads found! Returning null");
			return null;
		}
		LogicalGraph result = gc.getGraph(graphHead.get(0).getId());
		return result;
	}

	public LogicalGraph addLatestCommitOnThisBranchAsProperty(LogicalGraph g)
			throws Exception {
		MinVertexProperty mvp = new MinVertexProperty("time");
		LogicalGraph withMinTime = g.aggregate(mvp);
		int minTime = withMinTime.getGraphHead().collect().get(0).getPropertyValue(mvp.getAggregatePropertyKey()).getInt();
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
		LogicalGraph res =  g.transformGraphHead(new TransformationFunction<GraphHead>() {

			@Override
			public GraphHead apply(GraphHead current, GraphHead transformed) {
				transformed.setLabel(current.getLabel());
				transformed.setProperties(current.getProperties());
				transformed.setProperty(latestCommitHashLabel, latestCommitHash);
				return transformed;
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

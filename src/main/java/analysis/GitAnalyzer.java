package analysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.util.GradoopFlinkConfig;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

import gradoopify.GradoopFiller;

public class GitAnalyzer implements Serializable {

	private static final long serialVersionUID = 5400004312044745133L;

	public static final String branchGraphHeadLabel = "branch";
	public static final String latestCommitHashLabel = "latestCommitHash";
	public static final String commitVertexFieldUserEmail = "userEmail";

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

	/*
	 * Adds the authors email to all commits and then groups the 
	 * commits on that property
	 */
	public LogicalGraph groupCommitsByUser(LogicalGraph graph) throws Exception {
		List<Edge> graphEdges = graph.getEdges().collect();
		List<Vertex> graphVertices = graph.getVertices().collect();
		LogicalGraph enrichedGraph = graph.transformVertices(new TransformationFunction<Vertex>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Vertex apply(Vertex current, Vertex transformed) {
				if (current.getLabel().equals(GradoopFiller.commitVertexLabel)){
					for(Edge edge: graphEdges){
						GradoopId sourceId = edge.getSourceId();
						if(sourceId.equals(current.getId())){
							for (Vertex vertex:graphVertices){
								GradoopId targetId = edge.getTargetId();
								// if the target vertex is a user vertex
								if(targetId.equals(vertex.getId()) && vertex.getLabel().equals(GradoopFiller.userVertexLabel)){
									current.setProperty(commitVertexFieldUserEmail, vertex.getPropertyValue(GradoopFiller.userVertexFieldEmail));
								}
							}
						}
					}
				}
				
				return current;
			}
		});
		
		LogicalGraph onlyCommitsGraph = enrichedGraph.subgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(GradoopFiller.commitVertexLabel)) {
					return true;
				}
				return false;
			}
		}, new FilterFunction<Edge>() {

			@Override
			public boolean filter(Edge arg0) throws Exception {
				// TODO Auto-generated method stub
				return false;
			}
		});
		Grouping operator = new Grouping.GroupingBuilder()
				.setStrategy(GroupingStrategy.GROUP_REDUCE)
				.useVertexLabel(true)
				.addVertexGroupingKey(commitVertexFieldUserEmail)
				.addVertexAggregator(new CountAggregator("numberOfCommits"))
				.build();
		LogicalGraph grouped = onlyCommitsGraph.callForGraph(operator);
		return grouped;
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
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.branchVertexLabel);
			}

		}, new FilterFunction<Edge>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(Edge arg0) throws Exception {
				return false;
			}

		});
		List<Vertex> allBranches = onlyBranchVerticesGraph.getVertices().collect();
		List<Edge> allEdges = graph.getEdges().collect();
		List<LogicalGraph> resultGraphs = new ArrayList<LogicalGraph>();
		for (Vertex branch : allBranches) {
			LogicalGraph currentBranchSubGraph = graph.subgraph(new FilterFunction<Vertex>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

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

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public boolean filter(Edge e) throws Exception {
					if (e.getTargetId().equals(branch.getId())) {
						return true;
					}
					return false;
				}

			});
			currentBranchSubGraph = currentBranchSubGraph.transformGraphHead(new TransformationFunction<GraphHead>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public GraphHead apply(GraphHead current, GraphHead transformed) {
					current.setLabel(branchGraphHeadLabel);
					current.setProperty("name", branch.getPropertyValue("name").getString());
					return current;
				}

			});
		
			currentBranchSubGraph = addLatestCommitOnThisBranchAsProperty(currentBranchSubGraph);
			LogicalGraph newGraph = GradoopFiller.createGraphFromDataSetsAndAddThemToHead(
					currentBranchSubGraph.getGraphHead().collect().get(0), currentBranchSubGraph.getVertices(),
					currentBranchSubGraph.getEdges(), config);
			resultGraphs.add(newGraph);

		}
		GraphCollection result = GraphCollection.createEmptyCollection(config);
		for (LogicalGraph g : resultGraphs) {
			result = result.union(GraphCollection.fromGraph(g));
		}
		return result;
	}

	public LogicalGraph getGraphFromCollectionByBranchName(GraphCollection gc, String branchName) throws Exception {
		List<GraphHead> graphHead = gc.select(new FilterFunction<GraphHead>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

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

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

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

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

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
		LogicalGraph userCountGraph = ga.groupCommitsByUser(graph);
		// userCountGraph.getGraphHead().print();
		//GraphCollection branchGroupedGraph = ga.transformBranchesToSubgraphs(graph, gradoopConf);
		DataSink jsonSink = new JSONDataSink("./out", gradoopConf);
		userCountGraph.writeTo(jsonSink);
		env.execute();
	}
}

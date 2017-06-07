package analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.util.GradoopFlinkConfig;

import gradoopify.GradoopFiller;

public class GitAnalyzer {

	public LogicalGraph createUserCount(LogicalGraph graph){
		LogicalGraph userSubGraph = graph.subgraph(new FilterFunction<Vertex>(){

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.userVertexLabel);
			}
			
		}, new FilterFunction<Edge>() {

			@Override
			public boolean filter(Edge arg0) throws Exception {
				return false;
			}
			
		});
		return userSubGraph.aggregate(new VertexCount());
	}
	
	/**
	 * Adds Subgraphs to the graph which represent a branch.
	 * each of these branch subgraphs then contains all the commits belonging to the branch
	 * and the corresponding edges between the commits and the branch vertices.
	 */
	public LogicalGraph transforBranchesToSubgraphs(LogicalGraph graph) throws Exception{
		LogicalGraph onlyBranchVerticesGraph = graph.subgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.branchVertexLabel);
			}
			
		}, new FilterFunction<Edge>(){

			@Override
			public boolean filter(Edge arg0) throws Exception {
				return false;
			}
			
		});
		List<Vertex> allBranches = onlyBranchVerticesGraph.getVertices().collect();
		List<Edge> allEdges = graph.getEdges().collect();
		List<LogicalGraph> branchSubgraphs = new ArrayList<LogicalGraph>();
		for (Vertex branch : allBranches){
			LogicalGraph currentBranchSubGraph = graph.subgraph(new FilterFunction<Vertex>() {

				//Checks if the is an edge between the current vertex and current branch vertex
				@Override
				public boolean filter(Vertex v) throws Exception {
					for (Edge edge : allEdges){
						if (edge.getSourceId().equals(v.getId()) && edge.getTargetId().equals(branch.getId())){
							return true;
						}
					}
					return false;
				}
				
			}, new FilterFunction<Edge>(){

				@Override
				public boolean filter(Edge e) throws Exception {
					if (e.getTargetId().equals(branch.getId())) {
						return true;
					}
					return false;
				}
				
			});
			currentBranchSubGraph.transformGraphHead(new TransformationFunction<GraphHead>(){

				@Override
				public GraphHead apply(GraphHead current, GraphHead transformed) {
					current.setProperty("name", branch.getPropertyValue("name"));
					return current;
				}
				
			});
			branchSubgraphs.add(currentBranchSubGraph);
			System.out.println(branch.getPropertyValue("name"));
			System.out.println(currentBranchSubGraph.getGraphHead().collect().get(0).getPropertyValue("name"));
			currentBranchSubGraph.getGraphHead().print();
		}
		
		return graph;
	}
	
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(".");
		GitAnalyzer ga = new GitAnalyzer();
		LogicalGraph userCountGraph = ga.createUserCount(graph);
		userCountGraph.getGraphHead().print();
		LogicalGraph branchGroupedGraph = ga.transforBranchesToSubgraphs(graph);
		
	}
}

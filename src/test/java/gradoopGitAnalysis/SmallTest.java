package gradoopGitAnalysis;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Before;
import org.junit.Test;

import analysis.GitAnalyzer;
import gradoopify.GradoopFiller;

public class SmallTest {
	public ExecutionEnvironment env;
	public GradoopFlinkConfig config;
	public LogicalGraph testGraph;
	public GitAnalyzer analyzer;

	@Before
	public void setupData() throws Exception{
		env = ExecutionEnvironment.getExecutionEnvironment();
		config = GradoopFlinkConfig.createConfig(env);
		Properties branchProperties = new Properties();
		branchProperties.set("name", GitAnalyzerTest.branchName);
		Vertex branchVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchProperties);

		Properties branchUpProperties = new Properties();
		branchUpProperties.set("name", GitAnalyzerTest.branchUpName);
		Vertex branchUpVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchUpProperties);

		Properties userProperties = new Properties();
		userProperties.set("name", "Bob");
		userProperties.set("email", "Bob@example.com");
		userProperties.set("when", 1498033284);
		userProperties.set("timezone", 7200000);
		userProperties.set("timezoneOffset", 120);
		Vertex userVertex = config.getVertexFactory().createVertex(GradoopFiller.userVertexLabel, userProperties);
		
		
		Properties commitProperties = new Properties();
		commitProperties.set("name", GitAnalyzerTest.latestCommitHash);
		commitProperties.set("time", 1498033284);
		commitProperties.set("message", "Test commit");
		Vertex commitVertex = config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel, commitProperties);
		
		Edge edgeToUser = config.getEdgeFactory().createEdge(GradoopFiller.commitToUserEdgeLabel, commitVertex.getId(), userVertex.getId());
		Edge edgeToBranch = config.getEdgeFactory().createEdge(GradoopFiller.commitToBranchEdgeLabel, commitVertex.getId(), branchVertex.getId());
		Edge edgeToUpBranch = config.getEdgeFactory().createEdge(GradoopFiller.commitToBranchEdgeLabel, commitVertex.getId(), branchUpVertex.getId());

		
        DataSet<Vertex> testVertices = env.fromElements(branchVertex, branchUpVertex,userVertex,commitVertex);
        DataSet<Edge> testEdges = env.fromElements(edgeToBranch,edgeToUser, edgeToUpBranch);

		GraphHead testHead = new GraphHead(GradoopId.get(), "test Repo", new Properties());
		testVertices = addVertexDataSetToGraphHead(testHead,testVertices);
		testEdges = addEdgeDataSetToGraphHead(testHead,testEdges);

		testGraph = LogicalGraph.fromDataSets(env.fromElements(testHead),testVertices,testEdges,config);
		analyzer = new GitAnalyzer();
	}
	
	private DataSet<Vertex> addVertexDataSetToGraphHead(GraphHead graphHead, DataSet<Vertex> vertices){
        vertices = vertices
          .map(new AddToGraph<>(graphHead))
          .withForwardedFields("id;label;properties");
        return vertices;
	}

	private DataSet<Edge> addEdgeDataSetToGraphHead(GraphHead graphHead, DataSet<Edge> edges){
        edges = edges
          .map(new AddToGraph<>(graphHead))
          .withForwardedFields("id;sourceId;targetId;label;properties");
        return edges;
	}
	
	@Test
	public void testFilter() throws Exception{
		GradoopIdList list = new GradoopIdList();
		list.add(GradoopId.get());
		Vertex v1 = config.getVertexFactory().createVertex("Test1", new Properties(), list);
		Vertex v2 = config.getVertexFactory().createVertex("Test2", new Properties(), list);
		System.out.println(v1);
		System.out.println(v2);
		v2.setId(v1.getId());
		GradoopIdList list2 = new GradoopIdList();
		list2.addAll(list);
		list2.add(GradoopId.get());
		v2.setGraphIds(list2);
		System.out.println(v2);
		DataSet<Vertex> vertices = config.getExecutionEnvironment().fromElements(v1,v2);
		List<GradoopId> out = vertices.map(new Id<Vertex>()).collect();
		System.err.println(out);
		System.out.println(vertices.collect());
	}
	
	

	@Test
	public void testSubgraph() throws Exception{
		LogicalGraph g = testGraph.subgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.branchVertexLabel);
			}

		}, new FilterFunction<Edge>() {

			@Override
			public boolean filter(Edge arg0) throws Exception {
				return false;
			}

		});
		List<Vertex> branches = g.getVertices().collect();
		List<Edge> allEdges = testGraph.getEdges().collect();
		assertEquals(2,branches.size());
		Vertex branch = branches.get(0);
		assertEquals(4, testGraph.getVertices().collect().size());
		assertEquals(3, testGraph.getEdges().collect().size());
		LogicalGraph currentBranchSubGraph = testGraph.subgraph(new FilterFunction<Vertex>() {

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
					if (e.getTargetId().equals(branch.getId())) {
						return true;
					}
					return false;
				}

			});
		assertEquals(4, testGraph.getVertices().collect().size());
		assertEquals(3, testGraph.getEdges().collect().size());
		System.out.println(currentBranchSubGraph.getVertices().collect().size());
		System.out.println(currentBranchSubGraph.getGraphHead().collect().get(0).getId() + " " + currentBranchSubGraph.getVertices().collect().get(0).getGraphIds());
		System.out.println(currentBranchSubGraph.getEdges().collect().size());
		
		currentBranchSubGraph = analyzer.setBranchLabelAsGraphHeadProperty(currentBranchSubGraph, GitAnalyzerTest.branchName);
		currentBranchSubGraph = analyzer.addLatestCommitOnThisBranchAsProperty(currentBranchSubGraph);

		System.out.println("After");
		System.out.println(currentBranchSubGraph.getVertices().collect().size());

		List<Vertex> verticesInGraph = currentBranchSubGraph.getVertices().filter(new InGraph<>(currentBranchSubGraph.getGraphHead().collect().get(0).getId())).collect();
		System.out.println("InGrapH: " + verticesInGraph.size());
		System.out.println(currentBranchSubGraph.getGraphHead().collect().get(0).getId() + " " + currentBranchSubGraph.getVertices().collect().get(0).getGraphIds());
		System.out.println(currentBranchSubGraph.getEdges().collect().size());
	}
}

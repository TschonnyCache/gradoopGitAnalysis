package gradoopGitAnalysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Before;
import org.junit.Test;

import analysis.GitAnalyzer;
import gradoopify.GradoopFiller;

public class GitAnalyzerTest {
	GradoopFlinkConfig config;
	LogicalGraph testGraph;
	GraphCollection testCollection;
	GitAnalyzer analyzer;
	public static final String latestCommitHash = "fd9b02853a6bf1f123a350e536dfa90fdbe12f3b";
	public static final String branchName = "refs/heads/master";
	public static final String branchUpName = "refs/remotes/origin";
	
	@Before
	public void setupData() throws Exception{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		config = GradoopFlinkConfig.createConfig(env);
		Properties branchProperties = new Properties();
		branchProperties.set("name", branchName);
		Vertex branchVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchProperties);

		Properties branchUpProperties = new Properties();
		branchUpProperties.set("name", branchUpName);
		Vertex branchUpVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchUpProperties);

		Properties userProperties = new Properties();
		userProperties.set("name", "Bob");
		userProperties.set("email", "Bob@example.com");
		userProperties.set("when", 1498033284);
		userProperties.set("timezone", 7200000);
		userProperties.set("timezoneOffset", 120);
		Vertex userVertex = config.getVertexFactory().createVertex(GradoopFiller.userVertexLabel, userProperties);
		
		
		Properties commitProperties = new Properties();
		commitProperties.set("name", latestCommitHash);
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

		Properties gh1Properties= new Properties();
		gh1Properties.set("name", branchName);
		GraphHead gh1 = new GraphHead(GradoopId.get(), GitAnalyzer.branchGraphHeadLabel, gh1Properties);

		Properties gh2Properties= new Properties();
		gh2Properties.set("name", branchUpName);
		GraphHead gh2 = new GraphHead(GradoopId.get(), GitAnalyzer.branchGraphHeadLabel, gh2Properties);
		
		//Add vertices and edges to corresponding GraphHeads
		DataSet<Vertex> verticesForg1 = env.fromElements(branchVertex, userVertex, commitVertex);
		DataSet<Vertex> verticesForg2 = env.fromElements(branchUpVertex, userVertex, commitVertex);
		DataSet<Edge> edgesForg1 = env.fromElements(edgeToUser,edgeToBranch);
		DataSet<Edge> edgesForg2 = env.fromElements(edgeToUser,edgeToUpBranch);
		verticesForg1 = addVertexDataSetToGraphHead(gh1,verticesForg1);
		verticesForg2 = addVertexDataSetToGraphHead(gh2,verticesForg2);
		edgesForg1 = addEdgeDataSetToGraphHead(gh1,edgesForg1);
		edgesForg2 = addEdgeDataSetToGraphHead(gh2,edgesForg2);
		DataSet<Vertex> verticesForCollection = verticesForg1.union(verticesForg2);
		DataSet<Edge> edgesForCollection = edgesForg1.union(edgesForg2);
		
		testCollection = GraphCollection.fromDataSets(env.fromElements(gh1,gh2),verticesForCollection,edgesForCollection,config);
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
	public void transformBranchesToSubgraphsTest() throws Exception{
		GraphCollection transformed = analyzer.transformBranchesToSubgraphs(testGraph, config);
		List<GraphHead> ghs = transformed.getGraphHeads().collect();
		assertEquals(2,ghs.size());
		for(GraphHead gh: ghs){
			assertTrue(gh.getPropertyValue("name").getString().equals(branchName) || gh.getPropertyValue("name").getString().equals(branchUpName));
			assertEquals(latestCommitHash, gh.getPropertyValue(GitAnalyzer.latestCommitHashLabel).getString());
			List<Vertex> vertices = transformed.getVertices().filter(new InGraph<>(gh.getId())).collect();
			//Warum schlägt das hier fehl ?????
			assertEquals(3, vertices.size());
		}
	}
	
	@Test
	public void getGraphFromTransformedCollection() throws Exception{
		GraphCollection transformed = analyzer.transformBranchesToSubgraphs(testGraph, config);
		List<Vertex> tmp1 = transformed.getVertices().collect();
		List<Vertex> tmp2 = transformed.getVertices().filter(new InGraph<>(transformed.getGraphHeads().collect().get(0).getId())).collect();
		LogicalGraph out = analyzer.getGraphFromCollectionByBranchName(transformed, branchName);
		List<GraphHead> ghList = out.getGraphHead().collect();
		assertEquals(1,ghList.size());
		GraphHead gh =ghList.get(0); 
		assertEquals(GitAnalyzer.branchGraphHeadLabel, gh.getLabel());
		assertEquals(branchName, gh.getPropertyValue("name").getString());

		List<Vertex> vertices = out.getVertices().collect();
		assertEquals(3,vertices.size());
		for(Vertex v: vertices){
			if(v.getLabel().equals(GitAnalyzer.branchGraphHeadLabel)){
				assertEquals(branchName, v.getPropertyValue("name").getString());
			}
		}
		
		List<Edge> edges = out.getEdges().collect();
		assertEquals(2,edges.size());
	}
	
	@Test
	public void getGraphFromCollectionByBranchNameTest() throws Exception {
		LogicalGraph out = analyzer.getGraphFromCollectionByBranchName(testCollection, branchName);

		List<GraphHead> ghList = out.getGraphHead().collect();
		assertEquals(1,ghList.size());
		GraphHead gh =ghList.get(0); 
		assertEquals(GitAnalyzer.branchGraphHeadLabel, gh.getLabel());
		assertEquals(branchName, gh.getPropertyValue("name").getString());

		List<Vertex> vertices = out.getVertices().collect();
		assertEquals(3,vertices.size());
		for(Vertex v: vertices){
			if(v.getLabel().equals(GitAnalyzer.branchGraphHeadLabel)){
				assertEquals(branchName, v.getPropertyValue("name").getString());
			}
		}
		
		List<Edge> edges = out.getEdges().collect();
		assertEquals(2,edges.size());

	}

	@Test
	public void addLatestCommitOnThisBranchAsProperty() throws Exception{
		LogicalGraph out = analyzer.addLatestCommitOnThisBranchAsProperty(testGraph.copy());
		assertEquals(latestCommitHash, out.getGraphHead().collect().get(0).getPropertyValue(GitAnalyzer.latestCommitHashLabel).getString());
	}

	@Test
	public void createUserCount() throws Exception{
		LogicalGraph out = analyzer.createUserCount(testGraph.copy());
		out.getGraphHead().print();
		assertEquals(1, out.getGraphHead().collect().get(0).getPropertyValue("vertexCount").getLong());
	}
}

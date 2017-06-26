package gradoopGitAnalysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.revwalk.RevCommit;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import analysis.GitAnalyzer;
import gradoopify.GradoopFiller;

public class GradoopFillerTest {
	String pathToRepo = "src/test/resources/mockgit/";
	Git git;
	GradoopFlinkConfig config;
	Vertex branchVertex;
	Vertex userVertex;
	Vertex commitVertex;
	LogicalGraph testGraph;

	public static final String testGraphUserEmail = "bob@example.com";

	@Before
	public void setupData() throws IllegalStateException, GitAPIException, IOException {
		File repoDir = new File(pathToRepo);
		if (repoDir.listFiles() != null && repoDir.listFiles().length > 0) {
			FileUtils.deleteDirectory(repoDir);
		}
		git = Git.init().setDirectory(repoDir).call();
		String filePath = pathToRepo + "test.txt";
		FileWriter fw = new FileWriter(filePath);
		git.add().addFilepattern(filePath).call();
		RevCommit commit = git.commit().setMessage("Test commit").call();

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		config = GradoopFlinkConfig.createConfig(env);
		Properties branchProperties = new Properties();
		branchProperties.set("name", GitAnalyzerTest.branchName);
		branchVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchProperties);

		PersonIdent user = commit.getAuthorIdent();
		Properties userProperties = new Properties();
		userProperties.set("name", user.getName());
		userProperties.set("email", user.getEmailAddress());
		userProperties.set("when", user.getWhen().getTime());
		userProperties.set("timezone", user.getTimeZone().getRawOffset());
		userProperties.set("timezoneOffset", user.getTimeZoneOffset());
		userVertex = config.getVertexFactory().createVertex(GradoopFiller.userVertexLabel, userProperties);

		Properties commitProperties = new Properties();
		commitProperties.set("name", commit.name());
		commitProperties.set("time", commit.getCommitTime());
		commitProperties.set("message", commit.getShortMessage());
		commitVertex = config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel, commitProperties);

		testGraph = createTestGraph();
	}

	public LogicalGraph createTestGraph() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		config = GradoopFlinkConfig.createConfig(env);
		Properties branchProperties = new Properties();
		branchProperties.set("name", GitAnalyzerTest.branchName);
		Vertex branchVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchProperties);

		Properties userProperties = new Properties();
		userProperties.set("name", "Bob");
		userProperties.set("email", testGraphUserEmail);
		userProperties.set("when", 1498033284);
		userProperties.set("timezone", 7200000);
		userProperties.set("timezoneOffset", 120);
		Vertex userVertex = config.getVertexFactory().createVertex(GradoopFiller.userVertexLabel, userProperties);

		Properties commitProperties = new Properties();
		commitProperties.set("name", GitAnalyzerTest.latestCommitHash);
		commitProperties.set("time", 1498033284);
		commitProperties.set("message", "Test commit");
		Vertex commitVertex = config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel, commitProperties);

		Edge edgeToUser = config.getEdgeFactory().createEdge(GradoopFiller.commitToUserEdgeLabel, commitVertex.getId(),
				userVertex.getId());
		Edge edgeToBranch = config.getEdgeFactory().createEdge(GradoopFiller.commitToBranchEdgeLabel,
				commitVertex.getId(), branchVertex.getId());

		DataSet<Vertex> testVertices = env.fromElements(branchVertex, userVertex, commitVertex);
		DataSet<Edge> testEdges = env.fromElements(edgeToBranch, edgeToUser);

		GraphHead testHead = new GraphHead(GradoopId.get(), "test Repo", new Properties());
		testVertices = addVertexDataSetToGraphHead(testHead, testVertices);
		testEdges = addEdgeDataSetToGraphHead(testHead, testEdges);
		return LogicalGraph.fromDataSets(env.fromElements(testHead), testVertices, testEdges, config);
	}

	private DataSet<Vertex> addVertexDataSetToGraphHead(GraphHead graphHead, DataSet<Vertex> vertices) {
		vertices = vertices.map(new AddToGraph<>(graphHead)).withForwardedFields("id;label;properties");
		return vertices;
	}

	private DataSet<Edge> addEdgeDataSetToGraphHead(GraphHead graphHead, DataSet<Edge> edges) {
		edges = edges.map(new AddToGraph<>(graphHead)).withForwardedFields("id;sourceId;targetId;label;properties");
		return edges;
	}

	@Test
	public void readRepo() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);
		List<Vertex> vertices = graph.getVertices().collect();
		GradoopId edgeSource = null;
		GradoopId edgeToUserTarget = null;
		GradoopId edgeToBranchTarget = null;
		for (Vertex v : vertices) {
			switch (v.getLabel()) {
			case GradoopFiller.branchVertexLabel:
				assertEquals(branchVertex.getPropertyValue("name"), v.getPropertyValue("name"));
				edgeToBranchTarget = v.getId();
				break;
			case GradoopFiller.userVertexLabel:
				assertEquals(userVertex.getPropertyValue("name"), v.getPropertyValue("name"));
				assertEquals(userVertex.getPropertyValue("email"), v.getPropertyValue("email"));
				assertEquals(userVertex.getPropertyValue("when"), v.getPropertyValue("when"));
				assertEquals(userVertex.getPropertyValue("timezone"), v.getPropertyValue("timezone"));
				assertEquals(userVertex.getPropertyValue("timezoneOffset"), v.getPropertyValue("timezoneOffset"));
				edgeToUserTarget = v.getId();
				break;
			case GradoopFiller.commitVertexLabel:
				assertEquals(commitVertex.getPropertyValue("name"), v.getPropertyValue("name"));
				assertEquals(commitVertex.getPropertyValue("time"), v.getPropertyValue("time"));
				assertEquals(commitVertex.getPropertyValue("message"), v.getPropertyValue("message"));
				edgeSource = v.getId();
				break;
			}
		}
		List<Edge> edges = graph.getEdges().collect();
		for (Edge e : edges) {
			assertEquals(edgeSource, e.getSourceId());
			assertTrue(e.getTargetId().equals(edgeToUserTarget) || e.getTargetId().equals(edgeToBranchTarget));
		}
	}

	@Test
	public void readRepoTransformToSubgraphsAndGetBranch() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);

		GradoopId graphID = graph.getGraphHead().collect().get(0).getId();
		DataSet<Vertex> verticesDS = graph.getVertices().filter(new InGraph<>(graphID));
		List<Vertex> tmp = verticesDS.collect();

		GraphCollection collection = analyzer.transformBranchesToSubgraphs(graph, config);
		LogicalGraph branch = analyzer.getGraphFromCollectionByBranchName(collection, GitAnalyzerTest.branchName);
		List<Vertex> vertices = branch.getVertices().collect();
		assertEquals(1, vertices.size());
		assertEquals(1, collection.getEdges().collect().size());
	}

	@Test
	public void readUpdatedRepo() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);
		GraphCollection collection = analyzer.transformBranchesToSubgraphs(graph, config);
		List<Vertex> vertices = collection.getVertices().collect();
		assertEquals(1, vertices.size());
		assertEquals(1, collection.getEdges().collect().size());

		String filePath = pathToRepo + "test2.txt";
		FileWriter fw = new FileWriter(filePath);
		git.add().addFilepattern(filePath).call();
		RevCommit commit = git.commit().setMessage("Updated Commit test").call();

		Properties commitProperties = new Properties();
		commitProperties.set("name", commit.name());
		commitProperties.set("time", commit.getCommitTime());
		commitProperties.set("message", commit.getShortMessage());
		Vertex updatedCommitVertex = config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel,
				commitProperties);

		collection = gf.updateGraphCollection(pathToRepo, collection);

		List<Vertex> vUp = collection.getVertices().collect();
		List<Edge> eUp = collection.getEdges().collect();
		boolean foundNewVertex = false;
		for (Vertex v : collection.getVertices().collect()) {
			if (v.getLabel().equals(GradoopFiller.commitVertexLabel)
					&& v.getPropertyValue("time").equals(updatedCommitVertex.getPropertyValue("name"))) {
				foundNewVertex = true;
			}
		}
		assertTrue(foundNewVertex);

	}

	@Test
	public void getVertexFromDataSetTest() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		Vertex v = gf.getVertexFromDataSet(testGraphUserEmail, GradoopFiller.userVertexLabel,
				GradoopFiller.userVertexFieldEmail, testGraph.getVertices());
		assertNotNull(v);
		assertEquals(testGraphUserEmail, v.getPropertyValue(GradoopFiller.userVertexFieldEmail).getString());
	}

	@Test
	public void getVertexFromGraphTest() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		Vertex v = gf.getVertexFromGraph(testGraph, GradoopFiller.userVertexLabel, GradoopFiller.userVertexFieldEmail,
				testGraphUserEmail);
		assertNotNull(v);
		assertEquals(testGraphUserEmail, v.getPropertyValue(GradoopFiller.userVertexFieldEmail).getString());
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(new File(pathToRepo));
	}
}

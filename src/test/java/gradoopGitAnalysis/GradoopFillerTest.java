package gradoopGitAnalysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
	ExecutionEnvironment env;
	Vertex branchVertex;
	Vertex userVertex;
	ArrayList<Vertex> commitVertices = new ArrayList<Vertex>();
	LogicalGraph testGraph;
	
	public static int testRepoCommits = 0;
	public static int testRepoUser = 0;
	public static int testRepoBranches = 0;
	

	public static final String testGraphUserEmail = "bob@example.com";

	@Before
	public void setupData() throws IllegalStateException, GitAPIException, IOException {
		File repoDir = new File(pathToRepo);
		if (repoDir.listFiles() != null && repoDir.listFiles().length > 0) {
			FileUtils.deleteDirectory(repoDir);
		}
		git = Git.init().setDirectory(repoDir).call();
		ArrayList<RevCommit> commits = new ArrayList<RevCommit>();
		// creating some commits
		for (int i = 0; i<3; i++) {
			String filePath = pathToRepo + "test"+i+".txt";
			FileWriter fw = new FileWriter(filePath);
			git.add().addFilepattern(filePath).call();
			RevCommit commit = git.commit().setMessage("Test commit"+i+"").call();
			fw.close();
			commits.add(commit);
		}
		testRepoCommits = commits.size();


		env = ExecutionEnvironment.getExecutionEnvironment();
		config = GradoopFlinkConfig.createConfig(env);
		Properties branchProperties = new Properties();
		branchProperties.set("name", GitAnalyzerTest.branchName);
		branchVertex = config.getVertexFactory().createVertex(GradoopFiller.branchVertexLabel, branchProperties);
		//TODO still a magic number
		testRepoBranches = 1;

		PersonIdent user = commits.get(0).getAuthorIdent();
		Properties userProperties = new Properties();
		userProperties.set("name", user.getName());
		userProperties.set("email", user.getEmailAddress());
		userProperties.set("when", user.getWhen().getTime());
		userProperties.set("timezone", user.getTimeZone().getRawOffset());
		userProperties.set("timezoneOffset", user.getTimeZoneOffset());
		userVertex = config.getVertexFactory().createVertex(GradoopFiller.userVertexLabel, userProperties);
		//TODO still a magic number
		testRepoUser = 1;

		for (RevCommit commit: commits){
			Properties commitProperties = new Properties();
			commitProperties.set("name", commit.name());
			commitProperties.set("time", commit.getCommitTime());
			commitProperties.set("message", commit.getShortMessage());
			commitVertices.add(config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel, commitProperties));
		}
		// this is not related to the previous setup data and is only used in getVertexFromDataSetTest() and getVertexFromGraphTest()
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
		assertEquals(testRepoBranches + testRepoCommits + testRepoUser, vertices.size());
		//contains the gradoopIds of the commits, because all the edges start at the commits
		ArrayList<GradoopId> edgeSources = new ArrayList<GradoopId>();
		ArrayList<GradoopId> edgeTargets = new ArrayList<GradoopId>();
		for (Vertex v : vertices) {
			switch (v.getLabel()) {
			case GradoopFiller.branchVertexLabel:
				assertEquals(branchVertex.getPropertyValue("name"), v.getPropertyValue("name"));
				edgeTargets.add(v.getId());
				break;
			case GradoopFiller.userVertexLabel:
				assertEquals(userVertex.getPropertyValue("name"), v.getPropertyValue("name"));
				assertEquals(userVertex.getPropertyValue("email"), v.getPropertyValue("email"));
				assertEquals(userVertex.getPropertyValue("when"), v.getPropertyValue("when"));
				assertEquals(userVertex.getPropertyValue("timezone"), v.getPropertyValue("timezone"));
				assertEquals(userVertex.getPropertyValue("timezoneOffset"), v.getPropertyValue("timezoneOffset"));
				edgeTargets.add(v.getId());
				break;
			case GradoopFiller.commitVertexLabel:
				Vertex commitVertex = null;
				for (Vertex commitVertexEntry:commitVertices){
					if (commitVertexEntry.getPropertyValue("name").equals(v.getPropertyValue("name"))) {
						commitVertex = commitVertexEntry;
					}
				}
				assertNotNull(commitVertex);
				
				assertEquals(commitVertex.getPropertyValue("time"), v.getPropertyValue("time"));
				assertEquals(commitVertex.getPropertyValue("message"), v.getPropertyValue("message"));
				edgeSources.add(v.getId());
				//TODO only do this if it's not the initial commit
				edgeTargets.add(v.getId());
				break;
			}
		}
		List<Edge> edges = graph.getEdges().collect();
		//Edges are between: Commit/Branch, Commit/User, Commit/previousCommit
		assertEquals(testRepoBranches*testRepoCommits + testRepoCommits*testRepoUser + (testRepoCommits - 1), edges.size());
		for (Edge e : edges) {
			assertTrue(edgeSources.contains(e.getSourceId()));
			assertTrue(edgeTargets.contains(e.getTargetId()));
		}
	}

	@Test
	public void readRepoTransformToSubgraphsAndGetBranch() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);

		GraphCollection collection = analyzer.transformBranchesToSubgraphs(graph, config);
		LogicalGraph branch = analyzer.getGraphFromCollectionByBranchName(collection, GitAnalyzerTest.branchName);
		List<Vertex> vertices = branch.getVertices().collect();
		assertEquals(testRepoBranches + testRepoCommits + testRepoUser, vertices.size());
		assertEquals(testRepoBranches*testRepoCommits + testRepoCommits*testRepoUser + (testRepoCommits - 1), collection.getEdges().collect().size());
	}

	@Test
	public void readUpdatedRepo() throws Exception {
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);
		GraphCollection collection = analyzer.transformBranchesToSubgraphs(graph, config);
		List<Vertex> vertices = collection.getVertices().collect();
		assertEquals(testRepoBranches + testRepoCommits + testRepoUser, vertices.size());
		assertEquals(testRepoBranches*testRepoCommits + testRepoCommits*testRepoUser + (testRepoCommits - 1), collection.getEdges().collect().size());

		String filePath = pathToRepo + "testUpdate.txt";
		FileWriter fw = new FileWriter(filePath);
		git.add().addFilepattern(filePath).call();
		RevCommit commit = git.commit().setMessage("Updated Commit test").call();
		fw.close();
		String filePathBranch = pathToRepo + "testBranch.txt";
		FileWriter fwBranch = new FileWriter(filePathBranch);
		git.branchCreate().setName("testBranch").call();
		git.checkout().setName("testBranch").call();
		RevCommit branchCommit = git.commit().setMessage("commit on new branch").call();
		fwBranch.close();

		Properties commitProperties = new Properties();
		commitProperties.set("name", commit.name());
		commitProperties.set("time", commit.getCommitTime());
		commitProperties.set("message", commit.getShortMessage());
		Vertex updatedCommitVertex = config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel,
				commitProperties);

		collection = gf.updateGraphCollection(pathToRepo, collection);

		//Since we made 2 new commits and 1 new branch there should be 3 more
		assertEquals(testRepoBranches + testRepoCommits + testRepoUser + 3, collection.getVertices().collect().size());
		boolean foundNewVertex = false;
		List<Vertex> updatedVertices = collection.getVertices().collect();
		for (Vertex v : updatedVertices) {
			if (v.getLabel().equals(GradoopFiller.commitVertexLabel)
					&& v.getPropertyValue("name").equals(updatedCommitVertex.getPropertyValue("name"))) {
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
		Vertex v = gf.getVertexFromGraph(testGraph, GradoopFiller.userVertexLabel, GradoopFiller.userVertexFieldEmail, testGraphUserEmail);
		assertNotNull(v);
		assertEquals(testGraphUserEmail, v.getPropertyValue(GradoopFiller.userVertexFieldEmail).getString());
	}

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(new File(pathToRepo));
	}
}

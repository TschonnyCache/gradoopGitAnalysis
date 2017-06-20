package gradoopGitAnalysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.revwalk.RevCommit;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
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
	
	@Before
	public void setupData() throws IllegalStateException, GitAPIException, IOException{
		git = Git.init().setDirectory(new File(pathToRepo)).call();
		String filePath = pathToRepo + "test.txt";
		FileWriter fw = new FileWriter(filePath);
		git.add().addFilepattern(filePath).call();
		RevCommit commit = git.commit().setMessage("Test commit").call();

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		config = GradoopFlinkConfig.createConfig(env);
		Properties branchProperties = new Properties();
		branchProperties.set("name", "refs/heads/master");
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
	}
	
	@Test
	public void readRepo() throws Exception{
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);
		List<Vertex> vertices = graph.getVertices().collect();
		GradoopId edgeSource = null;
		GradoopId edgeToUserTarget = null;
		GradoopId edgeToBranchTarget = null;
		for(Vertex v : vertices){
			switch(v.getLabel()){
			case GradoopFiller.branchVertexLabel:
				assertEquals(branchVertex.getPropertyValue("name"),v.getPropertyValue("name"));
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
		for(Edge e: edges){
			assertEquals(edgeSource, e.getSourceId());
			assertTrue(e.getTargetId().equals(edgeToUserTarget) || e.getTargetId().equals(edgeToBranchTarget));
		}
	}

	@Test
	public void readUpdatedRepo() throws Exception{
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(config, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(pathToRepo);
		GraphCollection collection = analyzer.transformBranchesToSubgraphs(graph, config);
		
		String filePath = pathToRepo + "test2.txt";
		FileWriter fw = new FileWriter(filePath);
		git.add().addFilepattern(filePath).call();
		RevCommit commit = git.commit().setMessage("Updated Commit test").call();

		Properties commitProperties = new Properties();
		commitProperties.set("name", commit.name());
		commitProperties.set("time", commit.getCommitTime());
		commitProperties.set("message", commit.getShortMessage());
		Vertex updatedCommitVertex = config.getVertexFactory().createVertex(GradoopFiller.commitVertexLabel, commitProperties);
		
		collection = gf.updateGraphCollection(pathToRepo, collection);
		boolean foundNewVertex = false;
		for(Vertex v: collection.getVertices().collect()){
			if(v.getLabel().equals(GradoopFiller.commitVertexLabel) && v.getPropertyValue("time").equals(updatedCommitVertex.getPropertyValue("name"))){
				foundNewVertex = true;
			}
		}
		assertTrue(foundNewVertex);
		
	}
	
	@After
	public void cleanup() throws IOException{
		FileUtils.deleteDirectory(new File(pathToRepo));
	}
}

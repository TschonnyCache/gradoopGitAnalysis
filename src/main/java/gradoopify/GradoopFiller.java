package gradoopify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import io.LoadJGit;

public class GradoopFiller implements ProgramDescription {
	public static final String userVertexLabel = "user";
	public static final String branchVertexLabel = "branch";
	public static final String commitVertexLabel = "commit";
	public static final String commitToUserEdgeLabel ="commitToUserEdge";
	public static final String commitToBranchEdgeLabel ="commitToBranchEdge";
	private GradoopFlinkConfig config;
	
	private HashMap<String,Vertex> vertices = new HashMap<String,Vertex>();
	private List<Edge> edges = new ArrayList<Edge>();

	public GradoopFiller(GradoopFlinkConfig config) {
		this.config = config;
	}

	public Vertex createVertexFromUser(PersonIdent user) {
		Properties props = new Properties();
		props.set("name", user.getName());
		props.set("email", user.getEmailAddress());
		props.set("when", user.getWhen().getTime());
		props.set("timezone", user.getTimeZone().getRawOffset());
		props.set("timezoneOffset", user.getTimeZoneOffset());
		Vertex v = config.getVertexFactory().createVertex(userVertexLabel, props);
		vertices.put(user.getEmailAddress(),v);
		return v;
	}
	
	public Vertex createVertexFromBranch(Ref branch) {
		Properties props = new Properties();
		props.set("name",branch.getName());
		Vertex v = config.getVertexFactory().createVertex(branchVertexLabel,props);
		vertices.put(branch.getName(), v);
		return v;
	} 
	
	public Vertex createVertexFromCommit(RevCommit commit) {
		Properties props = new Properties();
		props.set("name",commit.name() );
		props.set("time",commit.getCommitTime());
		props.set("message",commit.getFullMessage());
		Vertex v = config.getVertexFactory().createVertex(commitVertexLabel,props);
		vertices.put(commit.name(), v);
		return v;
	}
	
	public Edge createEdgeToUser(RevCommit commit) {
		return config.getEdgeFactory().createEdge(commitToUserEdgeLabel, vertices.get(commit.getName()).getId(), vertices.get(commit.getAuthorIdent().getEmailAddress()).getId());
	}
	
	public Edge createEdgeToBranch(RevCommit commit, Ref branch){
		return config.getEdgeFactory().createEdge(commitToBranchEdgeLabel, vertices.get(commit.getName()).getId(), vertices.get(branch.getName()).getId());
	}
	
	public LogicalGraph parseGitRepoIntoGraph(String pathToGitRepo){
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo(pathToGitRepo);
		Map<String, Ref> mapRefs = ljg.getAllHeadsFromRepo(repository);
		try {
			List<Ref> branchs = new Git(repository).branchList().setListMode( ListMode.ALL ).call();
			for(Ref branch:branchs){
//				System.out.println("\n\n =====  Writing branch: " + branch.getName() + " =======\n\n");
				createVertexFromBranch(branch);
                try (RevWalk revWalk = new RevWalk(repository)) {
                    revWalk.markStart(revWalk.parseCommit(branch.getObjectId()));
                    RevCommit commit = revWalk.next();
                    while(commit != null){
//                    	System.out.println("Writing commit: " + commit.getShortMessage());
                        createVertexFromCommit(commit);
                        createVertexFromUser(commit.getAuthorIdent());
                        createEdgeToBranch(commit, branch);
                        createEdgeToUser(commit);
                        commit = revWalk.next();
                    }
                } catch (RevisionSyntaxException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (AmbiguousObjectException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IncorrectObjectTypeException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
			}

		} catch (GitAPIException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		LogicalGraph graph = LogicalGraph.fromCollections(vertices.values(), edges, config);
		return graph;
	}

	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LogicalGraph graph = gf.parseGitRepoIntoGraph("/home/ohdorno/git/spark");
		DataSet<Vertex> graphVertices = graph.getVertices();
		DataSet<Edge> graphEdges = graph.getEdges();
		try {
			graphVertices.print();
			graphEdges.print();
		} catch (Exception e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		CSVDataSink dataSink = new CSVDataSink("/home/ohdorno/git/gradoopGitAnalysis/output.csv", gradoopConf);
		try {
			graph.writeTo(dataSink);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}
}

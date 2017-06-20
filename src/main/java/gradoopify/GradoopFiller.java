package gradoopify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import io.LoadJGit;

public class GradoopFiller implements ProgramDescription {
	public static final String userVertexLabel = "user";
	public static final String branchVertexLabel = "branch";
	public static final String commitVertexLabel = "commit";
	public static final String commitToUserEdgeLabel = "commitToUserEdge";
	public static final String commitToBranchEdgeLabel = "commitToBranchEdge";
	private GradoopFlinkConfig config;

	private HashMap<String, Vertex> vertices = new HashMap<String, Vertex>();
	private HashMap<String, Edge> edges = new HashMap<String,Edge>();

	public GradoopFiller(GradoopFlinkConfig config) {
		this.config = config;
	}

	public Vertex createVertexFromUser(PersonIdent user) {
		Vertex v = vertices.get(user.getEmailAddress());
		if(v != null){
			return v;
		}
		Properties props = new Properties();
		props.set("name", user.getName());
		props.set("email", user.getEmailAddress());
		props.set("when", user.getWhen().getTime());
		props.set("timezone", user.getTimeZone().getRawOffset());
		props.set("timezoneOffset", user.getTimeZoneOffset());
		v = config.getVertexFactory().createVertex(userVertexLabel, props);
		vertices.put(user.getEmailAddress(), v);
		return v;
	}

	public Vertex createVertexFromBranch(Ref branch) {
		Vertex v = vertices.get(branch.getName());
		if(v != null){
			return v;
		}
		Properties props = new Properties();
		props.set("name", branch.getName());
		v = config.getVertexFactory().createVertex(branchVertexLabel, props);
		vertices.put(branch.getName(), v);
		return v;
	}

	public Vertex createVertexFromCommit(RevCommit commit) {
		Vertex v = vertices.get(commit.name());
		if(v != null){
			return v;
		}
		Properties props = new Properties();
		props.set("name", commit.name());
		props.set("time", commit.getCommitTime());
		props.set("message", commit.getShortMessage());
		v = config.getVertexFactory().createVertex(commitVertexLabel, props);
		vertices.put(commit.name(), v);
		return v;
	}

	public Edge createEdgeToUser(RevCommit commit) {
		Edge e = config.getEdgeFactory().createEdge(commitToUserEdgeLabel, vertices.get(commit.getName()).getId(),
				vertices.get(commit.getAuthorIdent().getEmailAddress()).getId());
		edges.put(commit.getName() + "->" + commit.getAuthorIdent(), e);
		return e;
	}

	public Edge createEdgeToBranch(RevCommit commit, Ref branch) {
		Edge e = config.getEdgeFactory().createEdge(commitToBranchEdgeLabel, vertices.get(commit.getName()).getId(),
				vertices.get(branch.getName()).getId());
		edges.put(commit.getName() + "->" + branch.getName(), e);
		if(commit.getName().equals("88b24790357370ff5012856d02c2a785b1720cec")){
			System.out.println(e.getId() + "     " + branch.getName());
			System.out.println("vid: " + vertices.get(commit.getName()).getId());
		}
		return e;
	}

	public Vertex getUserVertex(LogicalGraph graph, String userMail) throws Exception {
		LogicalGraph filtered = graph.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(GradoopFiller.userVertexLabel)) {
					if (v.getPropertyValue("email").equals(userMail)) {
						return true;
					}
				}
				return false;
			}
		});
		return filtered.getVertices().collect().get(0);
	}
	
	public Vertex getBranchVertex(LogicalGraph graph,String name) throws Exception{
		LogicalGraph filtered = graph.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(GradoopFiller.branchVertexLabel)) {
					if (v.getPropertyValue("name").equals(name)) {
						return true;
					}
				}
				return false;
			}
		});
		return filtered.getVertices().collect().get(0);
	}
	
	public LogicalGraph parseGitRepoIntoGraph(String pathToGitRepo) {
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo(pathToGitRepo);
		Map<String, Ref> mapRefs = ljg.getAllHeadsFromRepo(repository);
		try {
			Git git = new Git(repository);
			List<Ref> branchs = git.branchList().setListMode(ListMode.ALL).call();
			for (Ref branch : branchs) {
				// System.out.println("\n\n ===== Writing branch: " +
				// branch.getName() + " =======\n\n");
				createVertexFromBranch(branch);
				try (RevWalk revWalk = new RevWalk(repository)) {
					revWalk.markStart(revWalk.parseCommit(branch.getObjectId()));
					RevCommit commit = revWalk.next();
					while (commit != null) {
						// System.out.println("Writing commit: " +
						// commit.getShortMessage());
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
			git.close();
		} catch (GitAPIException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		LogicalGraph graph = LogicalGraph.fromCollections(new GraphHead(new GradoopId(), "test", new Properties()), vertices.values(), edges.values(), config);
		return graph;
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(".");
		DataSink sink = new JSONDataSink("./json",gradoopConf);
		sink.write(graph);
		env.execute();
		
	}

	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}
}

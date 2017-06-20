package gradoopify;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;


import analysis.GitAnalyzer;
import io.LoadJGit;

public class GradoopFiller implements ProgramDescription {
	public static final String userVertexLabel = "user";
	public static final String branchVertexLabel = "branch";
	public static final String commitVertexLabel = "commit";
	public static final String commitToUserEdgeLabel = "commitToUserEdge";
	public static final String commitToBranchEdgeLabel = "commitToBranchEdge";
	private GradoopFlinkConfig config;
	private GitAnalyzer analyzer;

	private HashMap<String, Vertex> vertices = new HashMap<String, Vertex>();
	private HashMap<String, Edge> edges = new HashMap<String, Edge>();
	
	private DataSet<Vertex> verticesDataSet = null;
	private DataSet edgesDataSet = null;

	public GradoopFiller(GradoopFlinkConfig config, GitAnalyzer analyzer) {
		this.config = config;
		this.analyzer = analyzer;
	}

	public GradoopFiller(GradoopFlinkConfig config) {
		this.config = config;
	}

	public Vertex createVertexFromUser(PersonIdent user) {
		Vertex v = vertices.get(user.getEmailAddress());
		if (v != null) {
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
		if (v != null) {
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
		if (v != null) {
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
		return e;
	}
	
	public Vertex getVertexFromGraph(LogicalGraph graph,String identifier) throws Exception{
		LogicalGraph filtered = graph.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(GradoopFiller.branchVertexLabel)) {
					if (v.hasProperty("name") && v.getPropertyValue("name").equals(identifier)) {
						return true;
					} else if (v.hasProperty("email") && v.getPropertyValue("email").equals(identifier)){
						return true;
					}
				}
				return false;
			}
		});
		if(filtered == null || filtered.getVertices().count()==0 || filtered.getVertices().count()>1){
			System.err.println("Too many or no vertices found in given graph! Returning null");
			return null;
		}
		return filtered.getVertices().collect().get(0);
	}
	
	public Vertex getVertexFromDataSet(String identifier, DataSet<Vertex> ds) throws Exception{
		DataSet<Vertex> filtered = ds.filter(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if(v.hasProperty("name") && v.getPropertyValue("name").equals(identifier)){
					return true;
				}else if(v.hasProperty("email") && v.getPropertyValue("email").equals(identifier)){
					return true;
				}
				return false;
			}
		});
		if (filtered == null || filtered.count() == 0 || filtered.count() > 1) {
			System.err.println("Too many or no vertices found in vertivesDataSet! Returning null");
			return null;
		}
		return filtered.collect().get(0);
	}
	
	public DataSet<Vertex> addUserVertexToDataSet(RevCommit c, LogicalGraph g, DataSet<Vertex> ds) throws Exception{
		PersonIdent user = c.getAuthorIdent();
		Vertex v = getVertexFromGraph(g, user.getEmailAddress());
		if(v == null){
            Properties props = new Properties();
            props.set("name", user.getName());
            props.set("email", user.getEmailAddress());
            props.set("when", user.getWhen().getTime());
            props.set("timezone", user.getTimeZone().getRawOffset());
            props.set("timezoneOffset", user.getTimeZoneOffset());
            v = config.getVertexFactory().createVertex(userVertexLabel, props);
		}
		return addVertexToDataSet(v, ds);
	}

	public DataSet<Vertex> addBranchVertexToDataSet(RevCommit c, LogicalGraph g, DataSet<Vertex> ds) throws Exception{
		Vertex v = getVertexFromGraph(g, c.getName());
		if(v == null){
            Properties props = new Properties();
            props.set("name", c.getName());
            v = config.getVertexFactory().createVertex(branchVertexLabel, props);
		}
		return addVertexToDataSet(v, ds);
	}

	public DataSet<Vertex> addCommitVertexToDataSet(RevCommit c, LogicalGraph g, DataSet<Vertex> ds) throws Exception{
		Vertex v = getVertexFromGraph(g, c.getName());
		if(v == null){
            Properties props = new Properties();
            props.set("name", c.name());
            props.set("time", c.getCommitTime());
            props.set("message", c.getShortMessage());
            v = config.getVertexFactory().createVertex(commitVertexLabel, props);
		}
		return addVertexToDataSet(v, ds);
	}
	
	public DataSet<Edge> addEdgeToBranchToDataSet(RevCommit c, Ref branch, LogicalGraph g, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromGraph(g, c.getName());
		if(source == null){
			source = getVertexFromDataSet(c.getName(), vertices);
		}
		Vertex target = getVertexFromGraph(g, branch.getName());
		Edge e = config.getEdgeFactory().createEdge(commitToBranchEdgeLabel, source.getId(), target.getId());
		return addEdgeToDataSet(e, edges);
	}
	
	public DataSet<Edge> addEdgeToUserToDataSet(RevCommit c, LogicalGraph g, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromGraph(g, c.getName()); 
		if(source == null){
			source = getVertexFromDataSet(c.getName(), vertices);
		}
		Vertex target = getVertexFromGraph(g, c.getAuthorIdent().getEmailAddress()); 
		if(target == null){
			target = getVertexFromDataSet(c.getAuthorIdent().getEmailAddress(), vertices);
		}
		Edge e = config.getEdgeFactory().createEdge(commitToUserEdgeLabel, source.getId(),target.getId());
		return addEdgeToDataSet(e, edges);
	}
	
	public DataSet<Vertex> addVertexToDataSet(Vertex v, DataSet<Vertex> ds){
		return ds.union(config.getExecutionEnvironment().fromElements(v));
	}

	public DataSet<Edge> addEdgeToDataSet(Edge e, DataSet<Edge> ds){
		return ds.union(config.getExecutionEnvironment().fromElements(e));
	}
	
	public LogicalGraph parseGitRepoIntoGraph(String pathToGitRepo) {
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo(pathToGitRepo);
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
		int lastPathCharIndex = new File(pathToGitRepo).getAbsolutePath().lastIndexOf(File.separator);
		LogicalGraph graph = LogicalGraph.fromCollections(new GraphHead(new GradoopId(),
				new File(pathToGitRepo).getAbsolutePath().substring(lastPathCharIndex), new Properties()),
				vertices.values(), edges.values(), config);
		return graph;
	}

	public GraphCollection updateGraphCollection(String pathToGitRepo, GraphCollection existingBranches) throws Exception {
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo(pathToGitRepo);
		try {
			GraphCollection updatedGraphs = GraphCollection.createEmptyCollection(config);
			Git git = new Git(repository);
			List<Ref> branchs = git.branchList().setListMode(ListMode.ALL).call();
			for (Ref branch : branchs) {
				String identifier = "";
				String latestCommitHash = "";
				LogicalGraph branchGraph = analyzer.getGraphFromCollectionByBranchName(existingBranches, identifier);
				if (branchGraph != null) {
					LogicalGraph newGraph = LogicalGraph.createEmptyGraph(config);
					DataSet<Vertex> newVertices = newGraph.getVertices();
					DataSet<Edge> newEdges = newGraph.getEdges();
					identifier = branch.getName();
					latestCommitHash = branchGraph.getGraphHead().collect().get(0)
							.getPropertyValue(GitAnalyzer.latestCommitHashLabel).getString();
					try (RevWalk revWalk = new RevWalk(repository)) {
						revWalk.markStart(revWalk.parseCommit(branch.getObjectId()));
						RevCommit commit = revWalk.next();
						while (commit != null && !commit.getName().toString().equals(latestCommitHash)) {
							newVertices = addCommitVertexToDataSet(commit, branchGraph, newVertices);
							newVertices = addUserVertexToDataSet(commit, branchGraph, newVertices);
							newEdges = addEdgeToBranchToDataSet(commit, branch, branchGraph, newEdges, newVertices);
							newEdges = addEdgeToUserToDataSet(commit, branchGraph, newEdges, newVertices);
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
					DataSet<GraphHead> gh = config.getExecutionEnvironment().fromElements(config.getGraphHeadFactory().createGraphHead());
					newGraph = LogicalGraph.fromDataSets(gh,newVertices,newEdges, config);
					branchGraph.combine(newGraph);
					existingBranches.union(GraphCollection.fromGraph(branchGraph));
				} else { // Create new branch
					createVertexFromBranch(branch);
					try (RevWalk revWalk = new RevWalk(repository)) {
						revWalk.markStart(revWalk.parseCommit(branch.getObjectId()));
						RevCommit commit = revWalk.next();
						while (commit != null) {
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
					Properties props = new Properties();
					props.set("name", branch.getName());
					props.set(GitAnalyzer.latestCommitHashLabel, latestCommitHash);
					GraphHead gh = config.getGraphHeadFactory().createGraphHead(GitAnalyzer.branchGraphHeadLabel, props);
					LogicalGraph newGraph = LogicalGraph.fromCollections(gh,vertices.values(), edges.values(), config);
					existingBranches.union(GraphCollection.fromGraph(newGraph));
				}
			}
			git.close();
		} catch (GitAPIException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return existingBranches;
	}

	private void putGraphEdgesAndVerticesToHashMap(LogicalGraph g, String identifier) throws Exception {
		List<Edge> edges = g.getEdges().collect();
		List<Vertex> vertices = g.getVertices().collect();
		for (Edge e : edges) {

		}

	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GitAnalyzer analyzer = new GitAnalyzer();
		GradoopFiller gf = new GradoopFiller(gradoopConf, analyzer);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(".");
		DataSink sink = new JSONDataSink("./json", gradoopConf);
		sink.write(graph);
		env.execute();

	}

	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}
}

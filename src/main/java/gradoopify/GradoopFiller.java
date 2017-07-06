package gradoopify;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
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
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import analysis.GitAnalyzer;
import io.LoadJGit;

public class GradoopFiller implements ProgramDescription {
	public static final String userVertexLabel = "user";
	public static final String branchVertexLabel = "branch";
	public static final String commitVertexLabel = "commit";
	public static final String commitToUserEdgeLabel = "commitToUserEdge";
	public static final String commitToBranchEdgeLabel = "commitToBranchEdge";
	public static final String commitToNextCommitEdgeLabel = "commitToNextCommitEdge";

	public static final String branchVertexFieldName = "name";

	public static final String commitVertexFieldName = "name";
	public static final String commitVertexFieldTime = "time";
	public static final String commitVertexFieldMessage = "message";

	public static final String userVertexFieldName = "name";
	public static final String userVertexFieldEmail = "email";
	public static final String userVertexFieldWhen = "when";
	public static final String userVertexFieldTimeZone = "timezone";
	public static final String userVertexFieldTimeZoneOffset = "timezoneOffset";

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
		props.set(userVertexFieldName, user.getName());
		props.set(userVertexFieldEmail, user.getEmailAddress());
		props.set(userVertexFieldWhen, user.getWhen().getTime());
		props.set(userVertexFieldTimeZone, user.getTimeZone().getRawOffset());
		props.set(userVertexFieldTimeZoneOffset, user.getTimeZoneOffset());
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
		props.set(branchVertexFieldName, branch.getName());
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
		props.set(commitVertexFieldName, commit.name());
		props.set(commitVertexFieldTime, commit.getCommitTime());
		props.set(commitVertexFieldMessage, commit.getShortMessage());
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
	
	public Edge createEdgeFromPreviousToLatestCommit(RevCommit previousCommit, RevCommit commit) {
		Edge e = config.getEdgeFactory().createEdge(commitToNextCommitEdgeLabel, vertices.get(previousCommit.getName()).getId(),
				vertices.get(commit.getName()).getId());
		edges.put(previousCommit.getName() + "->" + commit.getName(), e);
		return e;
	}
	
	public Vertex getVertexFromGraph(LogicalGraph graph, String label, String propertyKey, String identifier) throws Exception{
		LogicalGraph filtered = graph.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(label)) {
					if (v.hasProperty(propertyKey) && v.getPropertyValue(propertyKey).getString().equals(identifier)) {
						return true;
					}
				}
				return false;
			}
		});
		if(filtered == null || filtered.getVertices().count()==0){
			System.err.println("No vertices of type " + label + " with " + propertyKey + " = " + identifier + " found in given graph! Returning null");
			return null;
		}
		if(filtered.getVertices().count()>1){
			System.err.println("Too many vertices of type " + label + " with " + propertyKey + " = " + identifier + " found in given graph! Returning null");
			return null;
		}
		return filtered.getVertices().collect().get(0);
	}
	
	public Vertex getVertexFromDataSet(String identifier, String label, String propertyKey, DataSet<Vertex> ds) throws Exception{
		DataSet<Vertex> filtered = ds.filter(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(label)) {
					if (v.hasProperty(propertyKey) && v.getPropertyValue(propertyKey).getString().equals(identifier)) {
						return true;
					}
				}
				return false;
			}
		});
		if(filtered == null || filtered.count()==0){
			System.err.println("No vertices of type " + label + " with " + propertyKey + " = " + identifier + " found in given dataset! Returning null");
			return null;
		}
		if(filtered.count()>1){
			System.err.println("Too many vertices of type " + label + " with " + propertyKey + " = " + identifier + " found in given dataset! Returning null");
			return null;
		}
		return filtered.collect().get(0);
	}
	
	public DataSet<Vertex> addUserVertexToDataSet(RevCommit c, LogicalGraph g, DataSet<Vertex> ds) throws Exception{
		PersonIdent user = c.getAuthorIdent();
		Vertex v = getVertexFromGraph(g, userVertexLabel, userVertexFieldEmail, user.getEmailAddress());
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

	public DataSet<Vertex> addUserVertexToDataSet(RevCommit c, GraphCollection gc, DataSet<Vertex> ds) throws Exception{
		PersonIdent user = c.getAuthorIdent();
		Vertex v = getVertexFromDataSet(user.getEmailAddress(), userVertexLabel, userVertexFieldEmail, gc.getVertices());
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
		Vertex v = getVertexFromGraph(g, branchVertexLabel, branchVertexFieldName, c.getName());
		if(v == null){
            Properties props = new Properties();
            props.set("name", c.getName());
            v = config.getVertexFactory().createVertex(branchVertexLabel, props);
		}
		return addVertexToDataSet(v, ds);
	}

	public DataSet<Vertex> addCommitVertexToDataSet(RevCommit c, GraphCollection gc, DataSet<Vertex> ds) throws Exception{
		Vertex v = getVertexFromDataSet(c.getName(),commitVertexLabel,commitVertexFieldName,gc.getVertices());
		if(v == null){
            Properties props = new Properties();
            props.set("name", c.name());
            props.set("time", c.getCommitTime());
            props.set("message", c.getShortMessage());
            v = config.getVertexFactory().createVertex(commitVertexLabel, props);
		}
		return addVertexToDataSet(v, ds);
	}

	public DataSet<Vertex> addCommitVertexToDataSet(RevCommit c, LogicalGraph g, DataSet<Vertex> ds) throws Exception{
		Vertex v = getVertexFromGraph(g, commitVertexLabel, commitVertexFieldName, c.getName());
		if(v == null){
            Properties props = new Properties();
            props.set("name", c.name());
            props.set("time", c.getCommitTime());
            props.set("message", c.getShortMessage());
            v = config.getVertexFactory().createVertex(commitVertexLabel, props);
		}
		return addVertexToDataSet(v, ds);
	}

	public DataSet<Edge> addEdgeToBranchToDataSet(RevCommit c, Ref branch, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromDataSet(c.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		Vertex target = getVertexFromDataSet(branch.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		Edge e = config.getEdgeFactory().createEdge(commitToBranchEdgeLabel, source.getId(), target.getId());
		return addEdgeToDataSet(e, edges);
	}
	
	public DataSet<Edge> addEdgeToBranchToDataSet(RevCommit c, Ref branch, LogicalGraph g, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromGraph(g, commitVertexLabel, commitVertexFieldName, c.getName());
		if(source == null){
			source = getVertexFromDataSet(c.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		}
		Vertex target = getVertexFromGraph(g, branchVertexLabel, branchVertexFieldName, branch.getName());
		Edge e = config.getEdgeFactory().createEdge(commitToBranchEdgeLabel, source.getId(), target.getId());
		return addEdgeToDataSet(e, edges);
	}
	

	public DataSet<Edge> addEdgeToUserToDataSet(RevCommit c, GraphCollection gc, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromDataSet(c.getName(),commitVertexLabel,commitVertexFieldName,gc.getVertices());
		if(source == null){
			source = getVertexFromDataSet(c.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		}
		PersonIdent user = c.getAuthorIdent();
		Vertex target = getVertexFromDataSet(user.getEmailAddress(), userVertexLabel, userVertexFieldEmail, gc.getVertices());
		if(target == null){
			target = getVertexFromDataSet(c.getAuthorIdent().getEmailAddress(), userVertexLabel, userVertexFieldEmail, vertices);
		}
		Edge e = config.getEdgeFactory().createEdge(commitToUserEdgeLabel, source.getId(),target.getId());
		return addEdgeToDataSet(e, edges);
	}

	public DataSet<Edge> addEdgeToUserToDataSet(RevCommit c, LogicalGraph g, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromGraph(g, commitVertexLabel, commitVertexFieldName, c.getName()); 
		if(source == null){
			source = getVertexFromDataSet(c.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		}
		Vertex target = getVertexFromGraph(g,userVertexLabel, userVertexFieldEmail,  c.getAuthorIdent().getEmailAddress()); 
		if(target == null){
			target = getVertexFromDataSet(c.getAuthorIdent().getEmailAddress(), userVertexLabel, userVertexFieldEmail, vertices);
		}
		Edge e = config.getEdgeFactory().createEdge(commitToUserEdgeLabel, source.getId(),target.getId());
		return addEdgeToDataSet(e, edges);
	}
	
	public DataSet<Edge> addEdgeFromPreviousToLatestCommit(RevCommit commit,RevCommit previousCommit, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		Vertex source = getVertexFromDataSet(commit.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		Vertex target = getVertexFromDataSet(previousCommit.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		Edge e = config.getEdgeFactory().createEdge(commitToNextCommitEdgeLabel, source.getId(),target.getId());
		//returning the new edges dataSet containing the newly created edge
		return addEdgeToDataSet(e, edges);
	}

	public DataSet<Edge> addEdgeFromPreviousToLatestCommit(RevCommit commit,RevCommit previousCommit, LogicalGraph g, DataSet<Edge> edges, DataSet<Vertex> vertices) throws Exception{
		// trying to retrieve the commit from the existing graph. this could happen if the commit was introduced to the branch via merging another branch which contained the commit into the branch
		Vertex source = getVertexFromGraph(g, commitVertexLabel, commitVertexFieldName, commit.getName()); 
		// if the commit could not be retrieved, it is not yet part of the graph, but already part of the new vertices dataSet
		if(source == null){
			source = getVertexFromDataSet(commit.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		}
		Vertex target = getVertexFromGraph(g, commitVertexLabel, commitVertexFieldName, commit.getName()); 
		if(target == null){
			target = getVertexFromDataSet(previousCommit.getName(), commitVertexLabel, commitVertexFieldName, vertices);
		}
		
		Edge e = config.getEdgeFactory().createEdge(commitToNextCommitEdgeLabel, source.getId(),target.getId());
		//returning the new edges dataSet containing the newly created edge
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
			// the previously processed commit, i.e. one commit later in the revWalk as walks from the latest to the first commit
			RevCommit previousCommit = null;
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
						if (previousCommit != null) {
							createEdgeFromPreviousToLatestCommit(commit,previousCommit);
						}
						previousCommit = commit;
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
		GraphHead head = new GraphHead(GradoopId.get(), new File(pathToGitRepo).getAbsolutePath().substring(lastPathCharIndex), new Properties());
		LogicalGraph graph = createGraphFromCollectionsAndAddThemToHead(head, vertices.values(), edges.values(),config);
		return graph;
	}
	
	public static LogicalGraph createGraphFromCollectionsAndAddThemToHead(GraphHead head, Collection<Vertex> vertices, Collection<Edge> edges, GradoopFlinkConfig config){
		DataSet<Vertex> verticesDS = config.getExecutionEnvironment().fromCollection(vertices);
		DataSet<Edge> edgesDS = config.getExecutionEnvironment().fromCollection(edges);
		verticesDS = addVertexDataSetToGraphHead(head, verticesDS);
		edgesDS = addEdgeDataSetToGraphHead(head, edgesDS);
		return LogicalGraph.fromDataSets(config.getExecutionEnvironment().fromElements(head),verticesDS,edgesDS,config);
	}

	public static LogicalGraph createGraphFromDataSetsAndAddThemToHead(GraphHead head, DataSet<Vertex> vertices, DataSet<Edge> edges, GradoopFlinkConfig config){
		vertices = addVertexDataSetToGraphHead(head, vertices);
		edges = addEdgeDataSetToGraphHead(head, edges);
		return LogicalGraph.fromDataSets(config.getExecutionEnvironment().fromElements(head),vertices,edges,config);
	}

	private static DataSet<Vertex> addVertexDataSetToGraphHead(GraphHead graphHead, DataSet<Vertex> vertices){
        vertices = vertices
          .map(new AddToGraph<>(graphHead))
          .withForwardedFields("id;label;properties");
        return vertices;
	}

	private static DataSet<Edge> addEdgeDataSetToGraphHead(GraphHead graphHead, DataSet<Edge> edges){
        edges = edges
          .map(new AddToGraph<>(graphHead))
          .withForwardedFields("id;sourceId;targetId;label;properties");
        return edges;
	}

	public GraphCollection updateGraphCollection(String pathToGitRepo, GraphCollection existingBranches) throws Exception {
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo(pathToGitRepo);
		GraphCollection updatedGraphs = null;
		try {
			updatedGraphs = GraphCollection.createEmptyCollection(config);
			Git git = new Git(repository);
			List<Ref> branchs = git.branchList().setListMode(ListMode.ALL).call();
			for (Ref branch : branchs) {
				String identifier = branch.getName();
				String latestCommitHash = "";
				LogicalGraph branchGraph = analyzer.getGraphFromCollectionByBranchName(existingBranches, identifier);
				// if the branch is already present
				if (branchGraph != null) {
					LogicalGraph newGraph = LogicalGraph.createEmptyGraph(config);
					DataSet<Vertex> newVertices = newGraph.getVertices();
					DataSet<Edge> newEdges = newGraph.getEdges();
					RevCommit previousCommit = null;
					latestCommitHash = branchGraph.getGraphHead().collect().get(0)
							.getPropertyValue(GitAnalyzer.latestCommitHashLabel).getString();
					try (RevWalk revWalk = new RevWalk(repository)) {
						revWalk.markStart(revWalk.parseCommit(branch.getObjectId()));
						RevCommit commit = revWalk.next();
						// while the revwalk has not reached its end or the next commit is not the latest commit in the existing branch subgraph
						while (commit != null && !commit.getName().toString().equals(latestCommitHash)) {
							System.out.println(commit.getShortMessage());
							newVertices = addCommitVertexToDataSet(commit, branchGraph, newVertices);
							newVertices = addUserVertexToDataSet(commit, branchGraph, newVertices);
							newEdges = addEdgeToBranchToDataSet(commit, branch, branchGraph, newEdges, newVertices);
							newEdges = addEdgeToUserToDataSet(commit, branchGraph, newEdges, newVertices);
							if (previousCommit != null) {
								newEdges = addEdgeFromPreviousToLatestCommit(commit,previousCommit,branchGraph, newEdges, newVertices);
							}
							previousCommit = commit;
							commit = revWalk.next();
						}
						//connecting the latest parsed commit with the previous latest commit
						addEdgeFromPreviousToLatestCommit(commit,previousCommit, branchGraph, newEdges, newVertices);
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
					newGraph = createGraphFromDataSetsAndAddThemToHead(config.getGraphHeadFactory().createGraphHead(),newVertices,newEdges, config);
					branchGraph = branchGraph.combine(newGraph);
					DataSet<Vertex> updatedVertices = branchGraph.getVertices().union(existingBranches.getVertices()).distinct(new Id<Vertex>());
					DataSet<Edge> updatedEdges = branchGraph.getEdges().union(existingBranches.getEdges()).distinct(new Id<Edge>());
					DataSet<GraphHead> newGraphHead = branchGraph.getGraphHead();
					DataSet<GraphHead> updatedGraphHead = existingBranches.getGraphHeads().filter(g -> !g.getPropertyValue(GradoopFiller.branchVertexFieldName).equals(identifier));
					updatedGraphHead = updatedGraphHead.union(newGraphHead);
					updatedGraphs = GraphCollection.fromDataSets(updatedGraphHead, updatedVertices, updatedEdges, config);
				} else { // Create new branch
					LogicalGraph newBranchGraph = LogicalGraph.createEmptyGraph(config);
					DataSet<Edge> newEdges = newBranchGraph.getEdges();
					Vertex branchVertex = createVertexFromBranch(branch);
					DataSet<Vertex> newVertices = config.getExecutionEnvironment().fromElements(branchVertex);
					try (RevWalk revWalk = new RevWalk(repository)) {
						revWalk.markStart(revWalk.parseCommit(branch.getObjectId()));
						RevCommit commit = revWalk.next();
						while (commit != null) {
							newVertices = addCommitVertexToDataSet(commit, existingBranches, newVertices);
							newVertices = addUserVertexToDataSet(commit, existingBranches, newVertices);
							newEdges = addEdgeToBranchToDataSet(commit, branch, newEdges, newVertices);
							newEdges = addEdgeToUserToDataSet(commit, existingBranches, newEdges, newVertices);
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
					newBranchGraph = createGraphFromDataSetsAndAddThemToHead(gh,newVertices, newEdges, config);
					updatedGraphs = existingBranches.union(GraphCollection.fromGraph(newBranchGraph));
				}
			}
			git.close();
		} catch (GitAPIException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return updatedGraphs;
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

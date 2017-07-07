package analysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;

import gradoopify.GradoopFiller;

/**
 * @author jonas
 *
 */
public class GitAnalyzer implements Serializable {

	private static final long serialVersionUID = 5400004312044745133L;

	public static final String branchGraphHeadLabel = "branch";
	public static final String latestCommitHashLabel = "latestCommitHash";

	/**
	 * Aggregates the users using {@ 
	 * @param graph
	 * @return
	 */
	public LogicalGraph createUserCount(LogicalGraph graph) {
		return graph.aggregate(new UserCount());
	}
	
	public class UserCount extends Count implements VertexAggregateFunction{

		@Override
		public String getAggregatePropertyKey() {
			return "userCount";
		}

		@Override
		public PropertyValue getVertexIncrement(Vertex vertex) {
			if(vertex.getLabel().equals(GradoopFiller.userVertexLabel)){
				return PropertyValue.create(1L);
			}
			return PropertyValue.create(0L);
		}
		
	}
	
	/**
	 * Adds Subgraphs to the graph which represent a branch. each of these
	 * branch subgraphs then contains all the commits belonging to the branch, the branch vertex and user vertices
	 */
	public GraphCollection transformBranchesToSubgraphs(LogicalGraph graph, GradoopFlinkConfig config)
			throws Exception {
		LogicalGraph onlyBranchVerticesGraph = graph.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				return v.getLabel().equals(GradoopFiller.branchVertexLabel);
			}

		});
		List<Vertex> allBranches = onlyBranchVerticesGraph.getVertices().collect();
		List<LogicalGraph> resGraphs = new ArrayList<LogicalGraph>();
		for (Vertex branch : allBranches) {
			
			//Get edges to branch
			DataSet<Edge> edgesToBranch = graph.getEdges().filter(e -> {
				if (e.getTargetId().equals(branch.getId())) {
                    return true;
				}
				if(e.getLabel().equals(GradoopFiller.commitToUserEdgeLabel)){
					return true;
				}
				return false;
			});
			
			//Get vertices that belong to branches
			DataSet<Vertex> vertices = edgesToBranch
				.flatMap(new EdgeSourceTargetIds())
				.rightOuterJoin(graph.getVertices())
				.where(id -> id)
				.equalTo(v -> v.getId())
				.with(new FlatJoinFunction<GradoopId,Vertex,Vertex>() {

					@Override
					public void join(GradoopId id, Vertex v, Collector<Vertex> out) throws Exception {
						if(v.getId().equals(id)){
							out.collect(v);
						}
					}
				});
			
			//Remove duplicates
			vertices = vertices.distinct(new Id<Vertex>());

			//Add edges between final vertices
			//first by source
			DataSet<Edge> edgesBySource = vertices
					.map(new Id<Vertex>())
					.join(graph.getEdges())
					.where(id -> id)
					.equalTo(e -> e.getSourceId())
					.with(new FlatJoinFunction<GradoopId,Edge,Edge>() {

						@Override
						public void join(GradoopId id, Edge e, Collector<Edge> out) throws Exception {
							if(e.getSourceId().equals(id)){
								out.collect(e);
							}
						}
					});

			//Then by target
			DataSet<Edge> edgesByTarget = vertices
					.map(new Id<Vertex>())
					.join(graph.getEdges())
					.where(id -> id)
					.equalTo(e -> e.getTargetId())
					.with(new FlatJoinFunction<GradoopId,Edge,Edge>() {

						@Override
						public void join(GradoopId id, Edge e, Collector<Edge> out) throws Exception {
							if(e.getTargetId().equals(id)){
								out.collect(e);
							}
						}
					});
			//Then perform union on them
			DataSet<Edge> edges = edgesBySource.union(edgesByTarget);
			//Make sure to remove duplicates
			edges = edges.distinct(new Id<Edge>());

			Properties ghProps = new Properties();
			ghProps.set(GradoopFiller.branchVertexFieldName, branch.getPropertyValue(GradoopFiller.branchVertexFieldName).getString());
			GraphHead gh = new GraphHead(GradoopId.get(), GitAnalyzer.branchGraphHeadLabel, ghProps);
			LogicalGraph currentBranchSubGraph = GradoopFiller.createGraphFromDataSetsAndAddThemToHead(gh,vertices, edges, config);
			currentBranchSubGraph = addLatestCommitOnThisBranchAsProperty(currentBranchSubGraph);
			resGraphs.add(currentBranchSubGraph);
		}
		
		if(resGraphs.size() == 0){
			return null;
		}
        DataSet<Vertex> tmpVertices = resGraphs.get(0).getVertices();
        DataSet<Edge> tmpEdges = resGraphs.get(0).getEdges();
        List<GraphHead> tmpGraphHeads = new ArrayList<GraphHead>();
        tmpGraphHeads.add(resGraphs.get(0).getGraphHead().collect().get(0));

        //Fix bug that vertices with same Ids have different GradoopIdLists by joining them
        for(int i = 1; i < resGraphs.size(); i++){
        	LogicalGraph g = resGraphs.get(i);
        	tmpVertices = tmpVertices
        			.coGroup(g.getVertices())
        			.where(new Id<Vertex>())
        			.equalTo(new Id<Vertex>())
        			.with(new CoGroupFunction<Vertex,Vertex,Vertex>() {

						@Override
						public void coGroup(Iterable<Vertex> first, Iterable<Vertex> second, Collector<Vertex> out) throws Exception {
							//If any Iterable is empty we just return the other set
							Iterator<Vertex> firstIt = first.iterator();
							Iterator<Vertex> secondIt = second.iterator();
							if(!firstIt.hasNext()){
								while(secondIt.hasNext()){
									out.collect(secondIt.next());
								}
							}else if(!secondIt.hasNext()){
								while(firstIt.hasNext()){
									out.collect(firstIt.next());
								}
							}else{
                                //Else we join the GradoopIdLists
                                while(firstIt.hasNext()){
                                    Vertex v1 = firstIt.next();
                                    while(secondIt.hasNext()){
                                        Vertex v2 = secondIt.next();
                                        if(v1.getId().equals(v2.getId())){
                                            GradoopIdList firstIdList = v1.getGraphIds();
                                            for(GradoopId id: v2.getGraphIds()){
                                                if(!firstIdList.contains(id)){
                                                    firstIdList.add(id);
                                                }
                                            }
                                            v1.setGraphIds(firstIdList);
                                            out.collect(v1);
                                        }else{
                                            out.collect(v1);
                                            out.collect(v2);
                                        }
                                    }
                                }
                            }
						}
					});

            tmpEdges = tmpEdges
                .union(g.getEdges());
        	
        	tmpGraphHeads.add(g.getGraphHead().collect().get(0));
        }
        tmpVertices.print();
        GraphCollection result = GraphCollection.fromDataSets(config.getExecutionEnvironment().fromCollection(tmpGraphHeads), tmpVertices, tmpEdges, config);
		return result;
	}
	
	public class EdgeSourceTargetIds implements FlatMapFunction<Edge, GradoopId>{

		@Override
		public void flatMap(Edge e, Collector<GradoopId> out) throws Exception {
			out.collect(e.getSourceId());
			out.collect(e.getTargetId());
		}
	}

	/**
	 * @param g input graph
	 * @return the input graph with the @param branchName as a property with the key "name" on the graph head
	 */
	public LogicalGraph setBranchLabelAsGraphHeadProperty(LogicalGraph g, String branchName){
			LogicalGraph result = g.transformGraphHead(new TransformationFunction<GraphHead>() {

				@Override
				public GraphHead apply(GraphHead current, GraphHead transformed) {
					current.setLabel(branchGraphHeadLabel);
					current.setProperty("name", branchName);
					return current;
				}

			});
		return result;
	}

	/**
	 * For a given GraphCollection @param gc in which the subgraphs represent branches
	 * @return the subgraph with the @param branchName as property "name" on the graphHead
	 * @throws Exception null pointer exceptions occur if the input graph colleciton is empty
	 */
	public LogicalGraph getGraphFromCollectionByBranchName(GraphCollection gc, String branchName) throws Exception {
		// getting the graph head with the given branch name
		List<GraphHead> graphHead = gc.select(new FilterFunction<GraphHead>() {
			@Override
			public boolean filter(GraphHead entity) throws Exception {
				boolean correct = entity.hasProperty("name")
						&& entity.getPropertyValue("name").getString().equals(branchName);
				return correct;
			}
		}).getGraphHeads().collect();
		if (graphHead == null || graphHead.size() == 0 || graphHead.size() > 1) {
			System.err.println("Too many or no graph heads found! Returning null");
			return null;
		}
		GradoopId graphID = graphHead.get(0).getId();
		//extracting the corresponding vertices and edges from the input graph collection
		DataSet<Vertex> vertices = gc.getVertices().filter(new InGraph<>(graphID));
		DataSet<Edge> edges = gc.getEdges().filter(new InGraph<>(graphID));
		List<Vertex> test1 = vertices.collect();
		List<Edge> test2 = edges.collect();
		//creating a new graph from the graph head, vertices and edges
		LogicalGraph result = LogicalGraph.fromDataSets(gc.getConfig().getExecutionEnvironment().fromElements(graphHead.get(0)), vertices, edges, gc.getConfig());
		return result;
	}

	/**
	 * @param g the input graph, representing a branch subgraph
	 * @return the input graph, with the name of the latest commit as a property on the graph head
	 * @throws Exception
	 */
	public LogicalGraph addLatestCommitOnThisBranchAsProperty(LogicalGraph g) throws Exception {
		MinVertexProperty mvp = new MinVertexProperty("time");
		// annotating the smallest value of the property "time" of all vertices on the graph head 
		LogicalGraph withMinTime = g.aggregate(mvp);
		//extracting the value of the smallest time from the graphhead
		int minTime = withMinTime.getGraphHead().collect().get(0).getPropertyValue(mvp.getAggregatePropertyKey())
				.getInt();
		//getting the commit with this commit time
		LogicalGraph filtered = g.vertexInducedSubgraph(new FilterFunction<Vertex>() {

			@Override
			public boolean filter(Vertex v) throws Exception {
				if (v.getLabel().equals(GradoopFiller.commitVertexLabel)) {
					if (v.getPropertyValue("time").getInt() == minTime) {
						return true;
					}
				}
				return false;
			}
		});
		String latestCommitHash = filtered.getVertices().collect().get(0).getPropertyValue("name").getString();
		//annotating the name/hash of the latest commit on the graph head
		LogicalGraph res = g.transformGraphHead(new TransformationFunction<GraphHead>() {

			@Override
			public GraphHead apply(GraphHead current, GraphHead transformed) {
				current.setProperty(latestCommitHashLabel, latestCommitHash);
				return current;
			}

		});
		return res;
	}
	
	/**
	 * For a given @param graph, this function will group all commits depending on who the author is.
	 * The @return result is a LogicalGraph containing only n vertices, where n is the number users
	 * in the repository. These vertices represent the groups of commits and have a proper
	 * @param config needed to call annotateUserAndFilterCommits
	 * 
	 */
	public LogicalGraph groupCommitsByUser(LogicalGraph graph, GradoopFlinkConfig config) {
		graph = annotateUserAndFilterCommits(graph, config);
		Grouping operator = new Grouping.GroupingBuilder() 
				.useVertexLabel(true)
				.addVertexGroupingKey("userVertexId") 
				.addVertexAggregator(new CountAggregator("count")) 
				.setStrategy(GroupingStrategy.GROUP_COMBINE)
				.build();
		LogicalGraph grouped = graph.callForGraph(operator);
		return grouped;
	}
	
	/**
	 * @param the input graph
	 * @param config the config needet to create a new LogicalGraph as output.
	 * @return A subgraph containing only commit vertices, with the corresponding users gradoopId
	 */
	private LogicalGraph annotateUserAndFilterCommits(LogicalGraph graph, GradoopFlinkConfig config){
		DataSet<Vertex> vertices = graph.getVertices();
		//First we create tuples of commits and their corresponding user vertex gradoop id
		//This is done by joining the vertices with the edges and again joined with the vertices
		//so the resulting triples state vertex has an edge pointing to a vertex
		DataSet<Tuple2<Vertex,Vertex>> commitAndUserVertexTuples = vertices
			.join(graph.getEdges())
			.where(new Id<Vertex>())
			.equalTo(e -> e.getSourceId())
			.join(graph.getVertices())
			.where(new KeySelector<Tuple2<Vertex,Edge>, GradoopId>(){
	
				@Override
				public GradoopId getKey(Tuple2<Vertex, Edge> tuple) throws Exception {
					return tuple.f1.getTargetId();
				}
				
			})
			.equalTo(v -> v.getId())
			.with(new FlatJoinFunction<Tuple2<Vertex,Edge>, Vertex, Tuple2<Vertex,Vertex>>() {
	
				@Override
				public void join(Tuple2<Vertex, Edge> first, Vertex second, Collector<Tuple2<Vertex,Vertex>> out) throws Exception {
					//here we select only those tuples where a commit vertex is in relation to a user vertex
					if (first.f0.getLabel().equals(GradoopFiller.commitVertexLabel) && second.getLabel().equals(GradoopFiller.userVertexLabel)){
						out.collect(new Tuple2<Vertex,Vertex>(first.f0,second));
					}
				}
			});
		//here we map the tuples containing a commit and the corresponding user to only the commit vertices with a user vertex gradoop id as a property
		DataSet<Vertex> annotatedCommits = commitAndUserVertexTuples.map(new MapFunction<Tuple2<Vertex,Vertex>, Vertex>() {

			@Override
			public Vertex map(Tuple2<Vertex, Vertex> tuple) throws Exception {
				Vertex commit = tuple.f0;
				commit.setProperty("userVertexId", tuple.f1.getId());
				return commit;
			}
		});
		
		return LogicalGraph.fromDataSets(annotatedCommits, config);
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LogicalGraph graph = gf.parseGitRepoIntoGraph(".");
		GitAnalyzer ga = new GitAnalyzer();
//		GraphCollection branchGroupedGraph = ga.transformBranchesToSubgraphs(graph, gradoopConf);
		LogicalGraph userGroupedCommitsGraph = ga.groupCommitsByUser(graph, gradoopConf);
		DataSink jsonSink = new JSONDataSink("./json", gradoopConf);
		userGroupedCommitsGraph.writeTo(jsonSink);
		env.execute();
	}
}

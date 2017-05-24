package gradoopify;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand.ListMode;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.AmbiguousObjectException;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.RevisionSyntaxException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.util.GradoopFlinkConfig;

import io.LoadJGit;

public class GradoopFiller implements ProgramDescription {
	public static final String userVertexLabel = "user";
	public static final String branchVertexLabel = "branch";
	public static final String commitVertexLabel = "commit";
	private GradoopFlinkConfig config;

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
		return config.getVertexFactory().createVertex(userVertexLabel, props);
	}
	
	public Vertex createVertexFromBranch(Ref branch) {
		Properties props = new Properties();
		props.set("name",branch.getName());
		return config.getVertexFactory().createVertex(branchVertexLabel,props);
	} 
	
	public Vertex createVertexFromCommit(RevCommit commit) {
		Properties props = new Properties();
		props.set("name",commit.name() );
		props.set("time",commit.getCommitTime());
		props.set("message",commit.getFullMessage());
		return config.getVertexFactory().createVertex(commitVertexLabel,props);
	}

	public static void main(String[] args) {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo("/Users/jonas/Documents/Code/spark");
		Map<String, Ref> mapRefs = ljg.getAllHeadsFromRepo(repository);
		try {
			List<Ref> branchs = new Git(repository).branchList().setListMode( ListMode.ALL ).call();
			for(Ref branch:branchs){
				Vertex v = gf.createVertexFromBranch(branch);
			}
		} catch (GitAPIException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		try (RevWalk revWalk = new RevWalk(repository)) {
			ObjectId commitId;
			commitId = repository.resolve("refs/heads/master");
			revWalk.markStart(revWalk.parseCommit(commitId));
			Vertex vc = gf.createVertexFromCommit(revWalk.next());
			Vertex vu = gf.createVertexFromUser(revWalk.next().getAuthorIdent());
			System.out.println(vu);
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

	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}
}

package gradoopify;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
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

	public static void main(String[] args) {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gradoopConf = GradoopFlinkConfig.createConfig(env);
		GradoopFiller gf = new GradoopFiller(gradoopConf);
		LoadJGit ljg = new LoadJGit();
		Repository repository = ljg.openRepo("/home/ohdorno/git/spark");
		Map<String, Ref> mapRefs = ljg.getAllHeadsFromRepo(repository);

		try (RevWalk revWalk = new RevWalk(repository)) {
			ObjectId commitId;
			commitId = repository.resolve("refs/heads/master");
			revWalk.markStart(revWalk.parseCommit(commitId));
			Vertex v = gf.createVertexFromUser(revWalk.next().getAuthorIdent());
			System.out.println(v);
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

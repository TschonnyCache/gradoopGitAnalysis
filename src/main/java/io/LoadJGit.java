package io;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;

/**
 * Created by jonas on 11/05/17.
 */
public class LoadJGit {
	public static final String refConstraint = "refs/heads/";
	
	public Repository openRepo(String path){
		Repository repository = null;
		try {
			Git git = Git.open(new File(path));
			repository = git.getRepository();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return repository;
	}
	
	public Map<String,Ref> getAllHeadsFromRepo(String path){
		return getAllHeadsFromRepo(openRepo(path));
	}

	public Map<String,Ref> getAllHeadsFromRepo(Repository repo){
		Map<String,Ref> headsMap= new HashMap<String,Ref>();
		for(String s: repo.getAllRefs().keySet()){
			if(s.startsWith(refConstraint)){
				headsMap.put(s, repo.getAllRefs().get(s));
			}
		}
		return headsMap;
	}

}

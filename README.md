# gradoopGitAnalysis
An Analysis of Git repositories with the gradoop framework.

The tool provides methods to load a git repo (more precisely its meta data) into the EPGM data model of gradoop.
Commits, Branches, and Users are represented as vertices. Edges represent the realationships between the vertices.
An edge between a commit vertex and a branch vertex means that the commit is part of the branch. An edge between a
commit and a user means that the user is the author of the commit. An edge between commit A and commit B means
that commit A is a parent of commit B.

Also there is a method to transform the graph into a graph collection, where each subgraph represents a
branch of the repo. 
To avoid having to parse a repo again completely after new commits are introduced, the tool provides an update function.
Provided analysis functions are counting the users and grouping the commits by users and counting the commits of each user.

# couchUtils.R

This repository contains a file that I use everywhere in R whenever I
want to stash or retrieve stuff from CouchDB.

It contains some functions that help handle getting data in and out of
CouchDB.

Now that I've split it into its own repository, I am going to try to
generalize this a bit more, and maybe add tests...whatever that means
for R.  In the meantime, feel free to modify this code for your own
needs, but don't expect it to work for you out of the box as it
currently contains a lot of project-specific stuff.

With the version bump to 0.1.0, I've changed the way getting
attachments works.  Now it will load the file in the routine.  If this
fixes problems I've been having then all routines are likely to move
to this model.

# License


I guess the GPL v2 for now, as most of R is licensed like that.


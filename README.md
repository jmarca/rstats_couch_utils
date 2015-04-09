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

# Testing

testing is irritating.

You have to do this in R:

```
Sys.setenv(RCOUCHUTILS_TEST_CONFIG='/full/path/to/test.config.json')
```

or in bash

```
export RCOUCHUTILS_TEST_CONFIG='/full/path/to/test.config.json'
```

Then the test config file needs to point to a valid and running
couchdb instance as follows:

```
{
    "couchdb": {
        "host": "127.0.0.1",
        "port":5984,
        "trackingdb":"a_test_state_db",
        "auth":{"username":"myname",
                "password":"mypass"
               },
        "dbname":"testing",
        "design":"detectors",
        "view":"fips_year"
    }
}
```

Obviously change the name of your username and password to real ones.

# License


I guess the GPL v2 for now, as most of R is licensed like that.

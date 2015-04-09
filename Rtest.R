path <- normalizePath('./node_modules', winslash = "/", mustWork = FALSE)
lib_paths <- .libPaths()
.libPaths(c(path, lib_paths))
devtools::check()

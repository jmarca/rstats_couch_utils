## need node_modules directories
envrr <- Sys.getenv()
dot_is <- getwd()

plaba <- regexpr(pattern='/node_modules',envrr['PWD'])
if(plaba>0){
    ## stoppoint <- plaba + 12 ## same as  attr(plaba,'match.length')
    stoppoint <- plaba - 1 ## actually, go one up
    dot_is <- substr(envrr['PWD'],1,stoppoint)
}
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(paste(dot_is,'.Rlibs',sep='/')
                    , winslash = "/", mustWork = FALSE)
if(!file.exists(path)){
    dir.create(path)
}
lib_paths <- .libPaths()
.libPaths(c(path,node_paths,lib_paths))

## make sure remotes is installed
source("https://install-github.me/r-lib/remotes")

if(length(find.package("roxygen2",quiet=TRUE)) == 0){
    install.packages("roxygen2",repos="https://cloud.r-project.org",lib=path)
}
remotes::install_github("hadley/devtools")
remotes::install_github("tidyverse/magrittr")
devtools::install_deps(dependencies=TRUE)

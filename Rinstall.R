dot_is <- paste(getwd(),'..',sep='/')
envrr <- Sys.getenv()
dependencies <- grep(pattern='npm_package_rDependencies'
                    ,x=names(envrr),perl=TRUE,value=TRUE)
if('npm_config_root' %in% names(envrr)){
    ## operating under an NPM install
    print(paste('npm_config_root is',envrr$npm_config_root))
    print(paste('I think root dir is',dot_is))
}

node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(paste(dot_is,'.Rlibs',sep='/')
                    , winslash = "/", mustWork = FALSE)
if(!file.exists('path')){
    dir.create(path)
}
lib_paths <- .libPaths()
.libPaths(c(path,node_paths,lib_paths))
## ready to go
devtools::document()
devtools::install()

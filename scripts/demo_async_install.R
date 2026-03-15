source("R/async_install.R")

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Usage: Rscript scripts/demo_async_install.R <package> [fetcher-path] [lib-path]", call. = FALSE)
}

pkg <- args[[1]]
fetcher <- if (length(args) >= 2) args[[2]] else "./target/debug/async_dependency_installer_for_R"
lib <- if (length(args) >= 3) args[[3]] else NULL
result <- async_install_packages(
  packages = pkg,
  fetcher = fetcher,
  install_ncpus = 2L,
  make_jobs = 2L,
  download_concurrency = 16L,
  lib = lib
)

print(result$plan$layers)
print(vapply(result$fetch$results, function(entry) entry$status$kind, ""))

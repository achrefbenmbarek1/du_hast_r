suppressPackageStartupMessages({
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
results_csv <- if (length(args) >= 1) args[[1]] else NULL
if (is.null(results_csv)) {
  stop("Usage: Rscript scripts/summarize_benchmark_results.R <benchmark_results.csv> [output_dir]", call. = FALSE)
}
if (!file.exists(results_csv)) {
  stop(sprintf("File not found: %s", results_csv), call. = FALSE)
}

output_dir <- if (length(args) >= 2) args[[2]] else dirname(results_csv)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

raw <- read.csv(results_csv, stringsAsFactors = FALSE)

ok <- raw[raw$ok == TRUE & !is.na(raw$total_seconds), , drop = FALSE]
if (nrow(ok) == 0) {
  stop("No successful scenarios found in results file", call. = FALSE)
}

metric_median <- aggregate(
  total_seconds ~ stack + method + cache_state,
  data = ok,
  FUN = median
)

metric_min <- aggregate(
  total_seconds ~ stack + method + cache_state,
  data = ok,
  FUN = min
)

metric_max <- aggregate(
  total_seconds ~ stack + method + cache_state,
  data = ok,
  FUN = max
)

names(metric_median)[4] <- "median_total_seconds"
names(metric_min)[4] <- "min_total_seconds"
names(metric_max)[4] <- "max_total_seconds"

summary_table <- Reduce(function(a, b) merge(a, b, by = c("stack", "method", "cache_state"), all = TRUE),
                        list(metric_median, metric_min, metric_max))

compute_speedups <- function(df, target_method = "async") {
  rows <- list()
  idx <- 1L
  stacks <- unique(df$stack)
  states <- unique(df$cache_state)

  for (s in stacks) {
    for (state in states) {
      chunk <- df[df$stack == s & df$cache_state == state, , drop = FALSE]
      target <- chunk[chunk$method == target_method, , drop = FALSE]
      if (nrow(target) != 1 || is.na(target$median_total_seconds[[1]])) next
      target_time <- target$median_total_seconds[[1]]

      others <- chunk[chunk$method != target_method, , drop = FALSE]
      for (i in seq_len(nrow(others))) {
        baseline_time <- others$median_total_seconds[[i]]
        if (is.na(baseline_time) || baseline_time <= 0) next
        rows[[idx]] <- data.frame(
          stack = s,
          cache_state = state,
          baseline_method = others$method[[i]],
          async_median_seconds = target_time,
          baseline_median_seconds = baseline_time,
          speedup_x = baseline_time / target_time,
          reduction_pct = 100 * (baseline_time - target_time) / baseline_time,
          stringsAsFactors = FALSE
        )
        idx <- idx + 1L
      }
    }
  }

  if (length(rows) == 0) {
    return(data.frame())
  }
  do.call(rbind, rows)
}

speedups <- compute_speedups(summary_table)

summary_csv <- file.path(output_dir, "benchmark_summary.csv")
speedups_csv <- file.path(output_dir, "benchmark_speedups.csv")
summary_json <- file.path(output_dir, "benchmark_summary.json")

write.csv(summary_table, summary_csv, row.names = FALSE)
write.csv(speedups, speedups_csv, row.names = FALSE)

payload <- list(
  generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
  summary = summary_table,
  speedups = speedups
)
write_json(payload, summary_json, pretty = TRUE, auto_unbox = TRUE, null = "null")

cat(sprintf("Wrote summary: %s\n", summary_csv))
cat(sprintf("Wrote speedups: %s\n", speedups_csv))
cat(sprintf("Wrote summary JSON: %s\n", summary_json))

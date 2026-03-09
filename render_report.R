
args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  stop("No arguments supplied", call. = FALSE)
} else {
  # Assume the first argument is the value you want to pass
  argument_value <- args[1]
}

# Render the R Markdown file, overriding the 'my_arg' parameter
rmarkdown::render(
  input = "GenerateReport_cohort_linkage_QAQC.Rmd",
  params = list(file_name = argument_value)
)


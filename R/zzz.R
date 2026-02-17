# Internal environment where we store the Python module
.s4h_module <- NULL
.s4h_python_checked <- FALSE

# Internal function: check if Python and socio4health are available
.s4h_check_python <- function() {
  # Check if Python is available at all
  if (!reticulate::py_available()) {
    return(list(
      python_available = FALSE,
      socio4health_available = FALSE,
      message = "Python is not available in your system."
    ))
  }

  # Check if socio4health is installed
  socio4health_available <- reticulate::py_module_available("socio4health")

  return(list(
    python_available = TRUE,
    socio4health_available = socio4health_available,
    python_version = reticulate::py_version(),
    python_executable = reticulate::py_exe()
  ))
}

# Internal function: get the socio4health module (Python)
.s4h_get_module <- function() {
  if (is.null(.s4h_module)) {
    if (!reticulate::py_module_available("socio4health")) {
      stop(
        "The Python module 'socio4health' is not available in the current environment.\n",
        "To set up the Python environment automatically, run:\n",
        "  socio4healthR::s4h_setup()\n",
        "\n",
        "Or manually install the Python package:\n",
        "  pip install socio4health\n",
        "\n",
        "Or configure an existing environment where it is installed:\n",
        "  reticulate::use_condaenv('socio4health', required = TRUE)\n",
        "  reticulate::use_virtualenv('path/to/venv', required = TRUE)\n",
        "  reticulate::use_python('path/to/python.exe', required = TRUE)"
      )
    }
    .s4h_module <<- reticulate::import("socio4health", delay_load = TRUE)
  }
  .s4h_module
}

#' Set Up Python Environment for socio4healthR
#'
#' This function automatically installs the Python `socio4health` package
#' in a virtual environment or conda environment, or in the current Python installation.
#'
#' @param method Character string specifying installation method:
#'   \itemize{
#'     \item "auto" (default): Automatically detects and uses available method
#'     \item "virtualenv": Creates a virtual environment named "socio4health"
#'     \item "conda": Uses conda to create an environment named "socio4health"
#'     \item "pip": Installs directly to the current Python installation
#'   }
#' @param envname Character string name for the new environment (conda/virtualenv).
#'   Default: "socio4health"
#' @param python_version Python version to use (e.g., "3.10", "3.11").
#'   Only used with conda method.
#'
#' @details
#' This function handles the complete setup process:
#' 1. Checks if Python is installed
#' 2. Creates a virtual environment (if method requires it)
#' 3. Installs the socio4health Python package
#' 4. Configures reticulate to use the new environment
#'
#' @return Invisibly returns TRUE if setup was successful.
#'
#' @examples
#' \dontrun{
#' # Automatic setup (recommended)
#' socio4healthR::s4h_setup()
#'
#' # Setup with specific method
#' socio4healthR::s4h_setup(method = "virtualenv")
#' socio4healthR::s4h_setup(method = "conda", python_version = "3.11")
#' }
#'
#' @export
s4h_setup <- function(method = "auto", envname = "socio4health", python_version = NULL) {

  # Check if socio4health is already installed
  if (reticulate::py_module_available("socio4health")) {
    message("socio4health is already installed and available!")
    return(invisible(TRUE))
  }

  # Check if Python is available
  if (!reticulate::py_available()) {
    stop(
      "Python is not available on your system.\n",
      "Please install Python from https://www.python.org/ or use Anaconda/Miniconda.\n",
      "After installation, restart R and try again."
    )
  }

  message("Setting up Python environment for socio4health...")
  message("Python executable: ", reticulate::py_exe())
  message("Python version: ", reticulate::py_version())

  # Determine the installation method
  if (method == "auto") {
    # Try conda first if available, otherwise virtualenv
    if (nzchar(Sys.which("conda"))) {
      method <- "conda"
      message("Detected conda installation, using conda method")
    } else if (nzchar(Sys.which("python"))) {
      method <- "virtualenv"
      message("Detected Python installation, using virtualenv method")
    } else {
      method <- "pip"
      message("Using pip installation to current Python environment")
    }
  }

  # Install based on method
  if (method == "conda") {
    message("\nCreating conda environment '", envname, "'...")
    tryCatch({
      if (!is.null(python_version)) {
        reticulate::conda_create(envname, packages = paste0("python=", python_version))
      } else {
        reticulate::conda_create(envname)
      }
      message("Conda environment created")

      message("\nInstalling socio4health package...")
      reticulate::conda_install(envname, "socio4health")
      message("socio4health installed")

      message("\nConfiguring R to use the new environment...")
      reticulate::use_condaenv(envname, required = TRUE)
      message("Environment configured")
    }, error = function(e) {
      stop(
        "Error during conda setup:\n",
        conditionMessage(e),
        "\n\nPlease ensure conda is properly installed and try again."
      )
    })

  } else if (method == "virtualenv") {
    message("\nCreating virtual environment '", envname, "'...")
    tryCatch({
      reticulate::virtualenv_create(envname)
      message("Virtual environment created")

      message("\nInstalling socio4health package...")
      reticulate::virtualenv_install(envname, "socio4health")
      message("socio4health installed")

      message("\nConfiguring R to use the new environment...")
      reticulate::use_virtualenv(envname, required = TRUE)
      message("Environment configured")
    }, error = function(e) {
      stop(
        "Error during virtualenv setup:\n",
        conditionMessage(e),
        "\n\nPlease ensure Python is properly installed and try again."
      )
    })

  } else if (method == "pip") {
    message("\nInstalling socio4health to current Python environment...")
    tryCatch({
      reticulate::py_install("socio4health", pip = TRUE)
      message("socio4health installed")
    }, error = function(e) {
      stop(
        "Error during pip install:\n",
        conditionMessage(e),
        "\n\nPlease ensure pip is available and try again."
      )
    })
  } else {
    stop("Unknown installation method: ", method, ". Use 'auto', 'virtualenv', 'conda', or 'pip'.")
  }

  message("\nSetup complete! You can now use socio4healthR.")
  message("Restarting R is recommended to ensure the environment is properly loaded.")

  invisible(TRUE)
}

#' Check Python Environment Status
#'
#' Displays information about the current Python environment and socio4health availability.
#'
#' @return Invisibly returns a list with environment information.
#'
#' @examples
#' \dontrun{
#' s4h_check_env()
#' }
#'
#' @export
s4h_check_env <- function() {
  check_result <- .s4h_check_python()

  cat("Python Environment Status:\n")
  cat(paste(rep("-", nchar("Python Environment Status:")), collapse = ""), "\n")

  if (check_result$python_available) {
    cat("Python is available\n")
    cat("  Python version: ", check_result$python_version, "\n", sep = "")
    cat("  Python executable: ", check_result$python_executable, "\n", sep = "")

    if (check_result$socio4health_available) {
      cat("socio4health is installed and ready to use\n")
    } else {
      cat("socio4health is NOT installed\n")
      cat("  Run: socio4healthR::s4h_setup()\n")
    }
  } else {
    cat("Python is NOT available on your system\n")
    cat("  Please install Python from https://www.python.org/\n")
  }

  invisible(check_result)
}

#' @import reticulate
.onLoad <- function(libname, pkgname) {
  # Configure environment if specified
  if (Sys.getenv("SOCIO4HEALTH_CONDAENV") != "") {
    tryCatch({
      reticulate::use_condaenv(Sys.getenv("SOCIO4HEALTH_CONDAENV"), required = FALSE)
    }, error = function(e) {
      warning("Could not load specified conda environment: ", Sys.getenv("SOCIO4HEALTH_CONDAENV"))
    })
  }

  if (Sys.getenv("SOCIO4HEALTH_VIRTUALENV") != "") {
    tryCatch({
      reticulate::use_virtualenv(Sys.getenv("SOCIO4HEALTH_VIRTUALENV"), required = FALSE)
    }, error = function(e) {
      warning("Could not load specified virtual environment: ", Sys.getenv("SOCIO4HEALTH_VIRTUALENV"))
    })
  }

  if (Sys.getenv("RETICULATE_PYTHON") != "") {
    tryCatch({
      reticulate::use_python(Sys.getenv("RETICULATE_PYTHON"), required = FALSE)
    }, error = function(e) {
      warning("Could not use specified Python executable: ", Sys.getenv("RETICULATE_PYTHON"))
    })
  }

  # Try to load module silently
  .s4h_python_checked <<- TRUE
  tryCatch({
    .s4h_get_module()
  }, error = function(e) {
    # Silently continue; error will be raised when module is actually needed
  })
}

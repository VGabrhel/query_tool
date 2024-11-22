# make-scenario-categorization
## Repository structure
SQL queries should be located in `sql` directory. Python code is located in directory `src`, explaratory work in `notebooks` folder. 

## How to run it?
In order to run this locally, you need to have
1. Environmental variables set up
2. Virtual environment activated

### Environmental variables
Navigate to root folder. First, copy `.env.example` file and name it as `.env`. In this new file, set up variable values - feel free to reach to project authors for details. 

### Virtual Environment
The project uses [Poetry](https://python-poetry.org/) for dependency management. Once you have [Poetry](https://python-poetry.org/) installed, run `poetry install` in the root directory of the project. It will create a virtual environment and install all the dependencies. To access the virtual environment, run `poetry shell` in the directory.

#### Linters and Code Formatters
In project, we use [black](https://github.com/psf/black) and [flake8](https://flake8.pycqa.org/en/latest/). Both have corresponding configuration inside `pyproject.toml` file.

##### Run Black (Code formatter):

`black .`

##### Run Flake8 (Code formatter):

`flake8 --config pyproject.toml`

## How to contribute
1. Create a new git branch with the most accurate name, prefixed by Jira task ID.
2. Create a PR - at least one approval is needed. 

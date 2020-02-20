# ds_incubator
Incubator for data science projects.

Purpose:
 * scripts sharing between data scientist, and NOT for production push directly. 
 * Scripts ( mostly python) used for data exploration, model testing/verification...
 * Others Scripts you like to share or collaborate with team.
 * ... 

Feel free to build new sub-directory.

# Playground
## Jupyter Notebook
( the below scripts cloned from spark_jobs repo)

```shell
./dev-scripts/okta_spark jupyter-notebook
```
 
## IPython

```shell
./dev-scripts/okta_spark ipython
```


# install python 3
brew install pyenv
# it's recommended adding the following line to your shell rc
eval "$(pyenv init -)"

pyenv shell 3.7.4

virtualenv venv3
. venv3/bin/activate

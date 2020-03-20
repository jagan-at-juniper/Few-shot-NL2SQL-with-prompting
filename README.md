# ds_incubator
Incubator for data science projects.

Purpose:
 * scripts sharing between data scientist, and NOT for production push directly. 
 * Scripts ( mostly python) used for data exploration, model testing/verification...
 * Others Messy-intermediate codes you don't store locally.
 * ... 

Feel free to build your own sub-directory.

# Playground
## Jupyter Notebook
( the below scripts cloned from spark_jobs repo)

```shell
./dev-scripts/okta_spark jupyter-notebook
```

``shell
#./dev-scripts/okta_spark jupyter-notebook --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11
./dev-scripts/okta_spark jupyter-notebook --packages graphframes:graphframes:0.7.0-spark2.3-s_2.11
```
download  graphframes-0.7.0-spark2.3-s_2.11.jar .
 
to AWS athena, need ROLE of mist-data-science for perimission
``shell 
ROLE=mist-data-science ./dev-scripts/okta_spark jupyter-notebook
``shell
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

# Dbt DAG Parser
The Dbt DAG Parser is a Python library which converts Dbt DAGs to a NetworkX graph (specifically a directed graph or "DiGraph"). From here, it is pretty straightforward to convert a NetworkX graph to an Airflow DAG. Of course, you can use any type of Workflow Management System (WMS) you'd like.

For your convenience, there is a `convert_to_airflow_taskgroup()` method which can do this for you. Happy coding!


## Problem 
We would like to leverage Dbt in production. There is a lot that Dbt does well (like DAG generation). However, once Dbt generates a DAG, we want Airflow to handle the running of the DAG, not Dbt.

#### The Good – Dbt dag building
Dbt has a bunch of awesome utilities such as built-in *DAG builder*. This allows it to build the dependency graph via the `ref` function used in the models.

#### The Bad – Dbt dag running
Dbt also has a built-in *DAG runner*. However, Dbt’s DAG runner is missing some important features that come out-of-the-box with other WMSs (like Airflow). Further, by using Dbt's builtin DAG runner, we only have access to Dbt-specific tooling and miss out on things like:
- creating custom DAG structures
- perform inline testing
- using itermediate tooling like [Great Expectations](https://greatexpectations.io/)
- etc.


## Usage
```python3
from dbt_dag_parser import DbtDagParser

dag_parser = DbtDagParser(manifest_path="manifest.json")

# Access the parsed NetworkX graph directly.
graph = dag_parser.graph

# View nodes
print(graph.nodes())

```
>For more info on how to use NetworkX graphs, [here is a link to their docs](https://networkx.org/documentation/stable/tutorial.html#directed-graphs).


#### Convert to an Airflow DAG (well... a taskgroup)
To generate an Airflow Taskgroup from a our parser, you can run the following:
```python3
dag_parser = DbtDagParser(manifest_path="manifest.json")

with DAG(...) as dag:
    dbt_task_group = dag_parser.convert_to_airflow_dag(dag=dag)

    # You can treat an Airflow task as you would a list of operators
    dbt_task_group >> some_other_task
```

#### Tag Filtering
Within Dbt's models, you have the ability to add tags. If different portions of your DAG need to run on a different cadence, you can easily specify the Dbt models you want to include (while keeping the underlying structure) by tagging them (in the `config()` at the top of your model) and then using the following to do tag-based filtering:

```python3
dag_parser = DbtDagParser(
    manifest_path="manifest.json",
    dbt_tags=("mexico_project", "refresh:weekly")
)
```

The above example will find all nodes containing ALL of tags in the tuple ('mexico_project', 'refresh:weekly')

#### Final Thoughts:
The Dbt DAG parser depends on the `manifest.json` file that is created by running `dbt compile`.

If you have make changes to your Dbt models, you must rerun the `dbt compile` command for those changes to be reflected in the `manifest.json` file.

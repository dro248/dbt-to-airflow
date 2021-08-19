"""
Dbt Dag Parser:

The purpose of this parser is to leverage Dbt's automatic DAG generator to build Airflow DAGs.

By parsing the Dbt manifest.json file, we are able to build a graph using Python's NetworkX library.
We can then easily create it in Airflow.
"""
import json
import networkx as nx 


class DbtDagParser:
    """
    A utility class that parses out a dbt project and creates the respective task groups
    Args:
        manifest_path: (str) Path to the manifest.json file
        dbt_tags: (set) Filters out all dbt models that don't have all required tags
    """

    def __init__(
        self,
        manifest_path: str,
        dbt_tags: tuple = tuple(),
    ):
        self.graph = nx.DiGraph()
        self.manifest_path = manifest_path
        self.dbt_tags = set(dbt_tags)
        self.generate_graph()

    @staticmethod
    def load_dbt_manifest(manifest_path: str) -> dict:
        """
        Helper function to load the dbt manifest file
        Args:
            manifest_path: filepath to the manifest.json file
        Returns: A JSON object containing the dbt manifest content.
        """
        with open(manifest_path) as json_file:
            file_content = json.load(json_file)
        return file_content

    @staticmethod
    def get_model_name(node_name: str) -> str:
        """
        Extracts the model_name from the node_name
        Node names are of the form "model.project_name.model_name"
        Args:
            node_name: the full node name (e.g. "model.sdna_mx.dataplor")
        Returns: the model name extracted from the node_name (e.g. "dataplor")
        """
        return node_name.split(".")[-1]

    def generate_graph(self):
        """Parses the manifest.json file and uses it to populate a graph."""
        manifest_json = self.load_dbt_manifest(self.manifest_path)

        # Create the tasks for each model
        for node_name in manifest_json["nodes"].keys():
            model_name = self.get_model_name(node_name)

            if node_name.split(".")[0] == "model":
                tags = set(manifest_json["nodes"][node_name]["tags"])

                # Only use nodes with the right tag, if tag is specified
                if self.dbt_tags.issubset(tags):
                    self.graph.add_node(model_name)

        # Add upstream and downstream dependencies for each run task
        for node_name in manifest_json["nodes"].keys():
            model_name = self.get_model_name(node_name)
            tags = set(manifest_json["nodes"][node_name]["tags"])

            if node_name.split(".")[0] == "model" and self.dbt_tags.issubset(tags):

                for upstream_node in manifest_json["nodes"][node_name]["depends_on"]["nodes"]:
                    upstream_node_tags = set(manifest_json["nodes"][upstream_node]["tags"])

                    # add node hierarchy if both current-node and upstream-node have our filters
                    if upstream_node.split(".")[0] == "model" and self.dbt_tags.issubset(upstream_node_tags):
                        upstream_model = self.get_model_name(upstream_node)
                        self.graph.add_edges_from([(upstream_model, model_name)])

    def convert_to_airflow_taskgroup(self, dag, task_group_name: str = "dbt_group"):
        """
        Converts a NetworkX graph into an Airflow TaskGroup (basically a DAG).

        >Note: While this example code works, you may want to make the conversion yourself in order
        to do things like:
            - add inline testing
            - use a different operator
            - etc.

        Args:
            dag: a reference to the Airflow DAG
            task_group_name: the name of the Airflow TaskGroup which will be shown in the Airflow UI (optional)
        """
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup

        # This is the Airflow object that we are adding our Operators to (it's what we'll return)
        task_group = TaskGroup(task_group_name)

        # Create an Operator for all of the nodes in a way that we can reference them by node name (ie. "model name")
        task_lookup = {
            node: BashOperator(
                dag=dag,
                task_id=node,
                task_group=task_group,
                bash_command=f"dbt run --models {node}",
            )
            for node in self.graph.nodes()
        }

        # for every node in our task_dict...
        for node in task_lookup:
            # get list of current node's predecessors (i.e. "parent nodes")
            parent_nodes = list(self.graph.predecessors(node))

            # set all parent nodes upstream to the current node
            [task_lookup[parent_node] for parent_node in parent_nodes] >> task_lookup[node]

        return task_group

    def draw_graph(self, output_file: str = "dag.png") -> None:
        """
        Draws the graph to a PNG image.
        Args:
            output_file: the file name where the PNG image will be saved.
        """
        import matplotlib.pyplot as plt
        from networkx.drawing.nx_agraph import graphviz_layout

        plt.title("draw_networkx")
        plt.figure(figsize=(50, 50))
        pos = graphviz_layout(dag_parser.graph, prog="dot")
        nx.draw(self.graph, pos, with_labels=True, arrows=True)
        plt.savefig(output_file)


if __name__ == "__main__":

    MANIFEST_PATH = "manifest.json"
    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH, dbt_tags=("mx", "refresh_weekly"))
    dag_parser.draw_graph()

import json
import logging
import os
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
        dbt_tags: set = set(),
    ):
        self.graph = nx.DiGraph()
        self.manifest_path = manifest_path
        self.dbt_tags = dbt_tags
        self.generate_graph()

    def load_dbt_manifest(self):
        """
        Helper function to load the dbt manifest file.
        Returns: A JSON object containing the dbt manifest content.
        """
        manifest_path = os.path.join(self.manifest_path)
        with open(manifest_path) as f:
            file_content = json.load(f)
        return file_content

    def add_node(self, node_name):
        """
        Extracts the model_name from the node_name and adds it to the graph.
        Args:
            node_name: (str) The name of the node
        Returns: (str) the name of the model
        """
        model_name = node_name.split(".")[-1]

        # dbt_task = BashOperator(
        #     dag=self.dag,
        #     task_id=node_name,
        #     task_group=task_group,
        #     bash_command=f"\
        #         dbt {self.dbt_global_cli_flags} {dbt_verb} --target {self.dbt_target} --models {model_name} \
        #         --profiles-dir {self.dbt_profiles_dir} --project-dir {self.dbt_project_dir}",
        # )

        self.graph.add_node(model_name)

        # Keeping the log output, it's convenient to see when testing the python code outside of Airflow
        return model_name

    def generate_graph(self):
        """
        Parse out a JSON file and populates the task groups with dbt tasks
        Returns: None
        """
        manifest_json = self.load_dbt_manifest()
        dbt_tasks = {}

        # Create the tasks for each model
        for node_name in manifest_json["nodes"].keys():
            if node_name.split(".")[0] == "model":
                tags = manifest_json["nodes"][node_name]["tags"]
                # Only use nodes with the right tag, if tag is specified
                if self.dbt_tags.issubset(tags):
                    dbt_tasks[node_name] = self.add_node(node_name)

        # Add upstream and downstream dependencies for each run task
        for node_name in manifest_json["nodes"].keys():
            if node_name.split(".")[0] == "model":
                tags = manifest_json["nodes"][node_name]["tags"]

                # Only use nodes with the right tag, if tag is specified
                if self.dbt_tags.issubset(tags):
                    for upstream_node in manifest_json["nodes"][node_name]["depends_on"]["nodes"]:
                        upstream_node_type = upstream_node.split(".")[0]
                        if upstream_node_type == "model":
                            # dbt_tasks[upstream_node] >> dbt_tasks[node_name]
                            self.graph.add_edges_from([(dbt_tasks[upstream_node], dbt_tasks[node_name])])


if __name__ == "__main__":
    import matplotlib.pyplot as plt

    MANIFEST_PATH = "/Users/a80323573/Documents/pepsi/dbt_dag_parser/manifest.json"
    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH)

    nx.draw_spring(dag_parser.graph, node_size=60, font_size=4)
    plt.show()


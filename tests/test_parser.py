# IMPORT SHIM: added to make pytest understand path structure
import sys, os

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../")

# Imports
from dbt_dag_parser import DbtDagParser
import networkx as nx
import json

MANIFEST_PATH = "manifest.json"


#####################
#   PARSER tests    #
#####################


def test_parser__load_manifest_is_dict():
    manifest_obj = DbtDagParser.load_dbt_manifest(manifest_path=MANIFEST_PATH)

    assert isinstance(
        manifest_obj, dict
    ), f"""
        Expected: type(manifest_obj) -> dict
        Received: type(manifest_obj) -> {type(manifest_obj)}
        """


#####################
#   GRAPH tests     #
#####################


def test_graph__not_empty():
    """Validates that a graph is created and is not empty."""
    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH)

    assert dag_parser.graph.number_of_nodes() > 0


def test_graph__is_dag():
    """Validates that a graph is directed and acyclic (i.e. a DAG)."""
    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH)

    assert nx.is_directed_acyclic_graph(dag_parser.graph) is True


def test_graph__correct_structure():
    """
    Validates that the graph has the correct structure.
    """
    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH)
    graph = dag_parser.graph

    # check: inegi2_step1 is upstream inegi2_step2
    assert "inegi2_step1" in list(graph.predecessors("inegi2_step2"))

    # check: inegi2_step2 is upstream inegi2_step3
    assert "inegi2_step2" in list(graph.predecessors("inegi2_step3"))


def test_graph__all_nodes_exist():
    """
    Validates that our graph contains all of the nodes in the manifest.json file.
    """
    # Parse all node directly from the manifest
    with open(MANIFEST_PATH, "r") as jf:
        manifest_dict = json.load(jf)

    # this is ground truth
    all_nodes = set(
        [
            model.split(".")[-1]
            for model in manifest_dict["nodes"].keys()
            if model.startswith("model")
        ]
    )

    # generate the graph
    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH)

    all_nodes_in_graph = set(dag_parser.graph.nodes())

    assert (
        all_nodes_in_graph == all_nodes
    ), f"""
    Expected: all_nodes_in_graph == all_nodes
    Received:
        * len(all_nodes):                 {len(all_nodes)}
        * len(all_nodes_in_graph):        {len(all_nodes_in_graph)}
        * all_nodes - all_nodes_in_graph: {len(all_nodes - all_nodes_in_graph)}
    """


#####################
#   FILTER tests    #
#####################


def test_graph__filtered_nodes_one_filter():
    """
    Validate that the filter works.
    We need to be sure that:
    - that the correct nodes are in the graph
    - the hierarchy of nodes is correct
    """
    manifest_dict = DbtDagParser.load_dbt_manifest(manifest_path=MANIFEST_PATH)
    filter_tags = {"refresh_weekly"}

    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH, dbt_tags=filter_tags)
    graph = dag_parser.graph

    # ground truth for refresh_weekly tags
    all_refresh_weekly_nodes = set(
        [
            model.split(".")[-1]
            for model in manifest_dict["nodes"].keys()
            if filter_tags.issubset(set(manifest_dict["nodes"][model]["tags"]))
        ]
    )

    all_nodes_in_graph = set(graph.nodes())

    # Check: Validate that the graph is NOT empty
    assert len(all_nodes_in_graph) > 0, "Graph is empty!"

    assert (
        "mx_stores_stg" not in all_nodes_in_graph
    ), f"`mx_stores_stg` should not be in graph"

    assert "dataplor" in all_nodes_in_graph, f"`dataplor` should be in graph"

    # Check: Validate that the graph has EXACTLY the nodes we're expecting
    assert (
        all_refresh_weekly_nodes == all_nodes_in_graph
    ), "Graph didn't match what we expected."

    # check: dls_latam_ins_producto -> mx_sales_wk
    assert "dls_latam_ins_producto" in list(
        graph.predecessors("mx_sales_wk")
    ), f"""
    Incorrect structure.
    Expected: `dls_latam_ins_producto` to be upstream to `mx_sales_wk`
    """


def test_graph__filtered_nodes_multi_filter():
    """
    Validate that multiple filters work.
    We need to be sure that:
    - that the correct nodes are in the graph
    - the hierarchy of nodes is correct
    """
    manifest_dict = DbtDagParser.load_dbt_manifest(manifest_path=MANIFEST_PATH)
    filter_tags = {"mx", "refresh_weekly"}

    dag_parser = DbtDagParser(manifest_path=MANIFEST_PATH, dbt_tags=filter_tags)
    graph = dag_parser.graph

    # ground truth for refresh_weekly tags
    all_refresh_weekly_nodes = set(
        [
            model.split(".")[-1]
            for model in manifest_dict["nodes"].keys()
            if filter_tags.issubset(set(manifest_dict["nodes"][model]["tags"]))
        ]
    )

    all_nodes_in_graph = set(graph.nodes())

    # Check: Validate that the graph is NOT empty
    assert len(all_nodes_in_graph) > 0, "Graph is empty!"

    assert (
        "mx_stores_stg" not in all_nodes_in_graph
    ), f"`mx_stores_stg` should not be in graph"

    assert "dataplor" in all_nodes_in_graph, f"`dataplor` should be in graph"

    # Check: Validate that the graph has EXACTLY the nodes we're expecting
    assert (
        all_refresh_weekly_nodes == all_nodes_in_graph
    ), "Graph didn't match what we expected."

    # check: dls_latam_ins_producto -> mx_sales_wk
    assert "dls_latam_ins_producto" in list(
        graph.predecessors("mx_sales_wk")
    ), f"""
    Incorrect structure.
    Expected: `dls_latam_ins_producto` to be upstream to `mx_sales_wk`
    """

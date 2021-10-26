from elasticsearch.helpers import reindex

from config import get_es, CONFIG_FILE, logger
from util.config_util import load_config_for_site, get_index_from_yaml

es_client = get_es()

sites = get_index_from_yaml(CONFIG_FILE, "sites")

new_index = get_index_from_yaml(CONFIG_FILE, "es_index")
new_index_clone = get_index_from_yaml(CONFIG_FILE, "es_index_clone")
alias_name = get_index_from_yaml(CONFIG_FILE, "es_index_alias")
index_schema_file = get_index_from_yaml(CONFIG_FILE, "index_schema")


def get_list_of_indices(config_file):
    index_list = list()
    for site in sites:
        index_list.append(load_config_for_site(config_file, site, "index")["index_name"])

    return index_list


def create_new_index_from_schema(client, index_name, schema_file):
    delete_index(client, index_name)
    with open(schema_file) as index_file:
        source = index_file.read().strip()

        # Create a new index based on the index file
        client.indices.create(index=index_name, body=source)


def reindex_indices(client, source_indices_list, destination_index):
    for index in source_indices_list:
        reindex(client, index, destination_index)


def transfer_alias(client, alias, old_index_name, new_index_name):
    client.indices.update_aliases({
        "actions": [
            {"remove": {"index": old_index_name, "alias": alias}},
            {"add": {"index": new_index_name, "alias": alias}},
        ]
    })


def create_alias(alias, index):
    es_client.indices.update_aliases({
        "actions": [
            {"add": {"index": index, "alias": alias}},
        ]
    })


def delete_index(client, index):
    client.indices.delete(index=index, ignore=[404])


if __name__ == "__main__":
    logger.info(f"Creating alias {alias_name} for {new_index} index.")
    create_alias(alias_name, new_index)
    logger.info("Done.")

    # Create a new es index to merge the document indices
    logger.info(f"Creating the {new_index_clone} index.")
    create_new_index_from_schema(es_client, new_index_clone, index_schema_file)
    logger.info("Done.")

    # Merge the indices into the new index
    indices_to_reindex = get_list_of_indices(CONFIG_FILE)
    logger.info(f"Merging {indices_to_reindex} into {new_index_clone}.")
    reindex_indices(es_client, indices_to_reindex, new_index_clone)
    logger.info("Done.")

    logger.info(f"Updating alias to point to {new_index_clone}.")
    transfer_alias(es_client, alias_name, new_index, new_index_clone)
    logger.info("Done.")

    # Clone the new index to the final index
    logger.info(f"Creating the {new_index} index.")
    create_new_index_from_schema(es_client, new_index, index_schema_file)
    logger.info("Done.")
    logger.info(f"Cloning {new_index_clone} into {new_index}.")
    reindex_indices(es_client, [new_index_clone], new_index)
    logger.info("Done.")

    logger.info(f"Restoring the alias to point to {new_index}.")
    transfer_alias(es_client, alias_name, new_index_clone, new_index)
    logger.info("Done.")

    logger.info(f"Cleaning up redundant indices: {indices_to_reindex}, and {new_index_clone}.")
    for idx in indices_to_reindex:
        delete_index(es_client, idx)
    delete_index(es_client, new_index_clone)
    logger.info("Done.")

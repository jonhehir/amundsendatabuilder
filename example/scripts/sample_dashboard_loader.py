"""
This makes fake dashboards.
"""
# I don't need most of these imports, but I don't care.
import logging
import os
import sqlite3
import sys
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.csv_extractor import CsvTableColumnExtractor, CsvExtractor
from databuilder.extractor.neo4j_es_last_updated_extractor import Neo4jEsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.dashboard.dashboard_metadata import DashboardMetadata
from databuilder.publisher.elasticsearch_constants import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING

es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')

es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)

es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port)

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'


class TestDashboardExtractor(Extractor):
    def init(self, _conf):
        dashes = [
            DashboardMetadata(
                dashboard_group="Test Dashes",  # type: str
                dashboard_name=f"Test Dashboard {i}",  # type: str
                description="This is a test dashboard.",  # type: Union[str, None]
                tags=['test'],  # type: List
                cluster='prod',  # type: str
                product='testdash',  # type: Optional[str]
                dashboard_group_id='testdash',  # type: Optional[str]
                dashboard_id=i,  # type: Optional[str]
                dashboard_group_description="This is fake.",  # type: Optional[str]
                created_timestamp=1590590397,  # type: Optional[int]
                dashboard_group_url="https://example.com/",  # type: Optional[str]
                dashboard_url=f"https://example.com/?dashboard={i}",  # type: Optional[str]
            ) for i in range(5)
        ]
        self._iter = iter(dashes)

    def extract(self):
        try:
            return next(self._iter)
        except StopIteration:
            return None


def run_jobs():
    node_files_folder = '/var/tmp/amundsen/nodes'
    relationship_files_folder = '/var/tmp/amundsen/relationships'

    task = DefaultTask(extractor=TestDashboardExtractor(),
                       loader=FsNeo4jCSVLoader(),
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': uuid.uuid4(),  # should use unique tag here like {ds}
    })

    DefaultJob(conf=job_config,
               task=task,
               publisher=Neo4jCsvPublisher()).launch()

    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.entity_type': 'dashboard',
        'extractor.search_data.extractor.neo4j.graph_url': neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.model_class': "databuilder.models.dashboard_elasticsearch_document.DashboardESDocument",
        'extractor.search_data.extractor.neo4j.neo4j_auth_user': neo4j_user,
        'extractor.search_data.extractor.neo4j.neo4j_auth_pw': neo4j_password,
        'extractor.search_data.extractor.neo4j.neo4j_encrypted': False,
        'loader.filesystem.elasticsearch.file_path': extracted_search_data_path,
        'loader.filesystem.elasticsearch.mode': 'w',
        'publisher.elasticsearch.file_path': extracted_search_data_path,
        'publisher.elasticsearch.mode': 'r',
        'publisher.elasticsearch.client': elasticsearch_client,
        'publisher.elasticsearch.new_index': 'dashboard',
        'publisher.elasticsearch.doc_type': 'dashboard',
        'publisher.elasticsearch.alias': 'dashboard_search_index',
    })

    job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                    DASHBOARD_ELASTICSEARCH_INDEX_MAPPING)

    DefaultJob(conf=job_config,
               task=task,
               publisher=ElasticsearchPublisher()).launch()


if __name__ == "__main__":
    run_jobs()

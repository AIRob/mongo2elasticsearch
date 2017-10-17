from pymongo import MongoClient
from elasticsearch import Elasticsearch
import argparse
import configs
import json


NAME = 'Mongo2Elasticsearch'
VERSION = '0.1.0'
DESCR = 'Export data from MongoDB to Elasticsearch.'

class Transformer:

    def __init__(self):
        self.mongo = MongoClient(host=configs.MONGO_HOST, port=configs.MONGO_PORT)
        self.es = Elasticsearch(hosts=configs.ELASTICSEARCH_HOSTS)

    def load_mongo_data(self, database, collection, filters=None, projection=None, batch_num=100):
        conn = self.mongo.get_database(database).get_collection(collection)
        d = conn.find(filter=filters, projection=projection).batch_size(batch_num)
        return d

    def save_elasticsearch_data(self, cursor, index, doc_type, batch_num=100):
        body = []
        for data in cursor:
            body.append(json.dumps({"index": {"_index": index, "_type": doc_type, "_id": data['_id']}}))
            del data['_id']
            body.append(json.dumps(data))
            if len(body) >= batch_num * 2:
                self.bulk_elasticsearch(body, index=index, doc_type=doc_type)
                body = []
        if len(body) > 0:
            self.bulk_elasticsearch(body, index=index, doc_type=doc_type)

    def bulk_elasticsearch(self, body, index, doc_type):
        res = self.es.bulk('\n'.join(body), index=index, doc_type=doc_type)
        if res['errors']:
            print('Elasticsearch bulk errors')

    def run(self):
        tf = Transformer()
        for tm in configs.TRANSFORM:
            if 'mongo' not in tm or 'elasticsearch' not in tm:
                print("No mongo nor elasticsearch provided!")
                continue
            mongo = tm['mongo']
            es = tm['elasticsearch']
            cur = tf.load_mongo_data(mongo['database'], mongo['collection'], mongo['filter'], mongo['projection'], configs.BATCH_NUM)
            tf.save_elasticsearch_data(cur, es['index'], es['type'], configs.BATCH_NUM)


def init_argparse():
    parser = argparse.ArgumentParser(description=DESCR)
    parser.add_argument('-v', '--version', help='Show version', action='store_true')
    return parser.parse_args()


def show_version():
    print(NAME + ' Version ' + VERSION)


def main():
    args = init_argparse()
    if args.version:
        show_version()
        exit()
    tf = Transformer()
    tf.run()


if __name__ == '__main__':
    main()

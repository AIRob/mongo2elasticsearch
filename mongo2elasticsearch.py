from pymongo import MongoClient
from elasticsearch import Elasticsearch
import argparse
import configs
import json


NAME = 'Mongo2Elasticsearch'
VERSION = '0.1.0'
DESCR = 'Export data from MongoDB to Elasticsearch.'


class Transformer:

    def __init__(self, trans=None, **kwargs):
        """

        :param trans: transform json file
        :param batch_num: batch num for bulk
        """
        self.mongo = MongoClient(host=configs.MONGO_HOST, port=configs.MONGO_PORT)
        self.es = Elasticsearch(hosts=configs.ELASTICSEARCH_HOSTS)
        if 'batch_num' in kwargs and kwargs['batch_num']:
            self.batch_num = kwargs['batch_num']
        else:
            self.batch_num = configs.BATCH_NUM
        if trans is None:
            trans = configs.TRANSFORM_FILE
        with open(trans) as fh:
            self.transform = json.load(fh)

    def load_mongo_data(self, database, collection, filters=None, projection=None):
        conn = self.mongo.get_database(database).get_collection(collection)
        d = conn.find(filter=filters, projection=projection).batch_size(self.batch_num)
        print("Load data from Mongodb %s %s, total %d" % (database, collection, d.count()))
        return d

    def save_elasticsearch_data(self, cursor, index, doc_type):
        body = []
        for data in cursor:
            body.append(json.dumps({"index": {"_index": index, "_type": doc_type, "_id": str(data['_id'])}}))
            del data['_id']
            body.append(json.dumps(data))
            if len(body) >= self.batch_num * 2:
                self.bulk_elasticsearch(body, index=index, doc_type=doc_type)
                body = []
        if len(body) > 0:
            self.bulk_elasticsearch(body, index=index, doc_type=doc_type)

    def bulk_elasticsearch(self, body, index, doc_type):
        print('Index data to Elasticsearch %s %s, batch num %d' % (index, doc_type, len(body) / 2))
        res = self.es.bulk('\n'.join(body), index=index, doc_type=doc_type)
        if res['errors']:
            print('Elasticsearch bulk errors')

    def run(self):
        for tm in self.transform:
            if 'mongo' not in tm or 'elasticsearch' not in tm:
                print("No mongo nor elasticsearch provided!")
                continue
            mongo = tm['mongo']
            es = tm['elasticsearch']
            cur = self.load_mongo_data(mongo['database'], mongo['collection'], mongo['filter'], mongo['projection'])
            self.save_elasticsearch_data(cur, es['index'], es['type'])


def init_argparse():
    parser = argparse.ArgumentParser(description=DESCR)
    parser.add_argument('-v', '--version', help='Show version', action='store_true')
    parser.add_argument('-n', '--batch-num', help='Batch num for each bulk', type=int)
    parser.add_argument('-t', '--transform', help='Transform config file')
    return parser.parse_args()


def show_version():
    print(NAME + ' Version ' + VERSION)


def main():
    args = init_argparse()
    if args.version:
        show_version()
        exit()
    tf = Transformer(trans=args.transform, batch_num=args.batch_num)
    tf.run()


if __name__ == '__main__':
    main()

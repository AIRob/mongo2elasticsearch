# Mongodb config
MONGO_HOST = '192.168.0.21'
MONGO_PORT = 27017

# Elasticsearch config
ELASTICSEARCH_HOSTS = [{'host': '192.168.0.21', 'port': 9200}]

# Number of rows for each requests
BATCH_NUM = 500

# Data transform config
TRANSFORM = [
    # Each transform group contains mongo(source) and elasticsearch(target)
    {
        'mongo': {
            'database': '',
            'collection': '',
            'filter': None,  # filter for mongo find
            'projection': None  # projection for mongo fields
        },
        'elasticsearch': {
            'index': '',
            'type': ''
        }
    }
]

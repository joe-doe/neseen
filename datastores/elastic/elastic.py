import elasticsearch


def get_es():
    return elasticsearch.Elasticsearch()

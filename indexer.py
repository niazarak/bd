import argparse
import json

import pymongo


def main(args):
    mongo_address, mongo_port = args.mongo_address, args.mongo_port

    client = pymongo.MongoClient(mongo_address, mongo_port)
    db = client.get_database('startup_db')
    collection = db.get_collection('startups_coll')
    collection.drop()
    with open(args.input_file) as f:
        entries = []
        for line in f:
            entries.append(json.loads(line))
        collection.insert_many(entries)

    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', type=str, default='startups_with_founders.jsonl')
    parser.add_argument('--mongo_address', type=str, default='mongodb://localhost')
    parser.add_argument('--mongo_port', type=int, default=27017)
    args = parser.parse_args()
    main(args)

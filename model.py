#!/usr/bin/env python3
import datetime
import json

import pydgraph



def set_schema(client):
    schema = """
    type Person {
        name
        friend
        age
        married
        location
        dob
    }

    name: string @index(exact) .
    friend: [uid] @reverse .
    age: int .
    married: bool .
    location: geo .
    dob: datetime .
    """
    return client.alter(pydgraph.Operation(schema=schema))


def create_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        p = {
            'uid': '_:leo',
            'dgraph.type': 'Person',
            'name': 'Leo',
            'age': 39,
            'married': True,
            'location': {
                'type': 'Point',
                'coordinates': [-122.804489, 45.485168],
            },
            'dob': datetime.datetime(1984, 7, 9, 10, 0, 0, 0).isoformat(),
            'friend': [
                {
                    'uid': '_:tomasa',
                    'dgraph.type': 'Person',
                    'name': 'Tomasa',
                    'age': 13,
                }
            ],
            'school': [
                {
                    'name': 'ITESO',
                }
            ]
        }

        response = txn.mutate(set_obj=p)

        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def delete_person(client, name):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query search_person($a: string) {
            all(func: eq(name, $a)) {
               uid
            }
        }"""
        variables = {'$a': name}
        result = txn.query(query1, variables=variables)
        ppl = json.loads(result.json)
        for person in ppl['all']:
            print("UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print(f"{name} deleted")
        commit_response = txn.commit()
        print(commit_response)
    finally:
        txn.discard()


def search_person(client, name):
    query = """query search_person($a: string) {
        all(func: eq(name, $a)) {
            uid
            name
            age
            married
            location
            dob
            friend {
                name
                age
            }
            school {
                name
            }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people named {name}: {len(ppl['all'])}")
    print(f"Data associated with {name}:\n{json.dumps(ppl, indent=2)}")


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

#!/usr/bin/env python3
import datetime
import json
import csv


import pydgraph


#schema baby
def set_schema(client):
    schema = """
    type Artist {
        username
        followers
        monthly_listeners
        artist_has_album
        artist_has_track
    }

    type Album {
        name
        release_date
        genre
        album_has_track
    }

    type Track {
        name
        duration
        play_count
        popularity_score
        creation_date
    }

    type Playlist {
        name
        description
        followers
        creation_date
        playlist_has_song
        playlist_has_album
        playlist_has_artist
    }

    type User {
        username
        email
        age
        location
        subscription_type
        follow_user
        follow_playlist
        user_follows_track
    }

    username: string @index(exact, trigram) @unique .
    followers: int .
    monthly_listeners: int .

    name: string @index(exact, trigram) @unique .
    release_date: datetime .
    genre: string @index(hash) .
    duration: float .
    play_count: int .
    popularity_score: float .
    description: string .
    creation_date: datetime .
    email: string @index(exact) .
    age: int @index(int) .
    location: geo .
    subscription_type: string .

    follow_user: [uid] .
    follow_playlist: [uid] @reverse .
    user_follows_track: [uid] .
    playlist_has_song: [uid] .
    playlist_has_album: [uid] .
    playlist_has_artist: [uid] .
    album_has_track: [uid] .
    artist_has_album: [uid] .
    artist_has_track: [uid] .

    """
    return client.alter(pydgraph.Operation(schema=schema))


#sending to dgraph
def send_tracks_to_dgraph(client):
    try:
        with open("./csv_files/tracks.csv", 'r') as file:
            reader = csv.DictReader(file)
            
            # Prepare mutations for each track
            mutations = []
            for row in reader:
                mutation = {
                    "dgraph.type": "Track", #this is something chat refuses to add
                    "uid": "_:new_track",
                    "name": row["name"],
                    "duration": float(row["duration"]),
                    "play_count": int(row["play_count"]),
                    "popularity_score": float(row["popularity_score"]),
                    "creation_date": row["creation_date"],
                }
                mutations.append(mutation)
            
            # Create a new transaction.
            txn = client.txn()

            try:
                for mutation in mutations:
                    txn.mutate(set_obj=mutation)

                #commit transaction
                commit_response = txn.commit()
                print(f"Commit Response: {commit_response}")
            finally:
                txn.discard()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        txn.discard()
def send_albums_to_dgraph(client):
    albums = []
    
    # Step 1: Read the CSV file
    with open("./csv_files/albums.csv", "r") as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            album_name = row["name"]
            release_date = row["release_date"]
            genre = row["genre"]
            album_tracks = row["album_has_track"].strip("[]").split("+")
           
            
            # Step 2: Query Dgraph for track UIDs
            
            track_uids = []
            for track_name in album_tracks:
                tracks = query_track_by_name(client, track_name.strip())
                if tracks:
                    track_uids.append(tracks[0]["uid"])  # Get UID of the first match (assuming unique names)
            
            
            # Step 3: Create album mutation data
            album_data = {
                "dgraph.type": "Album",
                "uid": "_:new_album",  # Blank node for new album
                "name": album_name,
                "release_date": release_date,
                "genre": genre,
                "album_has_track": track_uids  # Link tracks to the album via UIDs
            }
            
            
            albums.append(album_data)
    
    # Step 4: Send mutations to Dgraph
    txn = client.txn()
    try:
        for album in albums:
            txn.mutate(set_obj=album)
        txn.commit()
        print(f"Successfully added {len(albums)} albums to Dgraph.")
    finally:
        txn.discard()


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

#example
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

#actual queries
def query_track_by_name(client, track_name):
    query = """
    query TrackQuery($name: string) {
        tracks(func: eq(name, $name)) {
            uid
            name
            duration
            play_count
            popularity_score
            creation_date
        }
    }
    """
    
    variables = {"$name": track_name}
    response = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(response.json)
    
    # Return the tracks from the response
    return data.get("tracks", [])

def query_album_by_name(client, album_name):
    query = """
    query AlbumQuery($name: string) {
        albums(func: eq(name, $name)) {
            uid
            name
            release_date
            genre
        }
    }
    """
    
    variables = {"$name": album_name}
    print("Running query for album:", album_name)
    
    # Perform the query
    response = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(response.json)
    return data.get("albums", [])

def query_by_age(client, age):
    query = """
    query SearchByAge($age: int) {
        all(func: eq(age, $age)) {
            uid
            name
            age
        }
    }
    """
    variables = {"$age": age}
    response = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(response.json)
    return data.get("all", [])

def query_reversed_relationship(client, playlist_name):
    query = """
    query ReversedFollowPlaylist($name: string) {
        playlists(func: eq(name, $name)) {
            name
            follow_playlist {
                name
            }
        }
    }
    """
    variables = {"$name": playlist_name}
    response = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(response.json)
    return data.get("playlists", [])

def query_track_count(client):
    query = """
    query TrackCount {
        tracks(func: has(name)) {
            count(uid)
        }
    }
    """
    response = client.txn(read_only=True).query(query)
    data = json.loads(response.json)
    return data.get("tracks", [])

def delete_person_by_name(client, name):
    txn = client.txn()
    try:
        query = """query search_person($a: string) {
            all(func: eq(name, $a)) {
                uid
            }
        }"""
        variables = {'$a': name}
        result = txn.query(query, variables=variables)
        ppl = json.loads(result.json)
        for person in ppl['all']:
            txn.mutate(del_obj=person)
        txn.commit()
        print(f"Deleted person: {name}")
    finally:
        txn.discard()


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

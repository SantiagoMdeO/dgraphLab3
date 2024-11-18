#!/usr/bin/env python3

#im not going to lie teach, i tried my best in inputing the data, but noticed i was running out of time

#i really understood the part of the parsing data, as i really did entertain myself with theat

#but i cant say the same about the queries, tried my best but i ran out of time, yep


import os

import pydgraph

import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def print_menu():
    print("""
Option 1 -- Create data
Option 2 -- Search person
Option 3 -- Delete person
Option 4 -- Drop All
Option 5 -- Exit
Option 5: Queries the track by name.
Option 6: Queries the album by name.
Option 7: Queries people by age. 
Option 8: Queries the reversed relationship for playlists. 
Option 9: Queries count functions. 
Option 10: delete by condition person. 
          

    """)


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema ---------------------creating sequence every time we start
    model.drop_all(client) #drop all so schema can execute correctly
    model.set_schema(client)
    model.send_tracks_to_dgraph(client)
    model.send_albums_to_dgraph(client)

    while True:
        print_menu()
        option = int(input('Enter your choice: '))

        if option == 1:
            model.create_data(client)

        elif option == 2:
            person = input("Name: ")
            model.search_person(client, person)

        elif option == 3:
            person = input("Name: ")
            model.delete_person(client, person)

        elif option == 4:
            model.drop_all(client)

        elif option == 5:
            track_name = input("Enter track name: ")
            tracks = model.query_track_by_name(client, track_name)
            if tracks:
                for track in tracks:
                    print(f"Track: {track['name']}, Duration: {track['duration']}, Play Count: {track['play_count']}, Popularity: {track['popularity_score']}")
            else:
                print(f"No tracks found with the name '{track_name}'.")

        elif option == 6:
            album_name = input("Enter album name: ")
            albums = model.query_album_by_name(client, album_name)
            if albums:
                for album in albums:
                    print(f"Album: {album['name']}, Release Date: {album['release_date']}, Genre: {album['genre']}")
            else:
                print(f"No albums found with the name '{album_name}'.")

        elif option == 7:
            age = int(input("Enter age: "))
            people = model.query_by_age(client, age)
            if people:
                for person in people:
                    print(f"Name: {person['name']}, Age: {person['age']}")
            else:
                print(f"No people found with age '{age}'.")

        elif option == 8:
            playlist_name = input("Enter playlist name: ")
            playlists = model.query_reversed_relationship(client, playlist_name)
            if playlists:
                for playlist in playlists:
                    print(f"Playlist: {playlist['name']}, Followers: {[follower['name'] for follower in playlist.get('follow_playlist', [])]}")
            else:
                print(f"No playlists found with the name '{playlist_name}'.")

        elif option == 9:
            track_count = model.query_track_count(client)
            print(f"Total number of tracks: {track_count[0]['count']}" if track_count else "No tracks found.")

        elif option == 10:
            person_name = input("Enter the name of the person to delete: ")
            model.delete_person_by_name(client, person_name)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))
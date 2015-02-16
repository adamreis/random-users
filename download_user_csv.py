#!/usr/bin/env python3
__author__ = 'Adam Reis'
__copyright__ = 'Copyright 2015, Adam Reis'
__license__ = 'MIT'

import requests
from multiprocessing import Process, Pipe, cpu_count
from sys import argv, exit

def download_worker(n_users, conn):
    users = []

    for i in range(n_users):
        error = True
        while error:
            r = requests.get('http://api.randomuser.me/?format=csv')

            # This means there was an error. Trust me.
            # I should just be able to check the response code, but they always respond with 200.
            error = r.text[-1] == '}' 
        users.append(r.text)


    # Write this portion of the users to the main thread
    conn.send(users)
    conn.close()

def download_n_users(n_users, out_filename):
    num_workers = cpu_count()*4
    num_per_worker = n_users//num_workers

    # Create a few jobs and start them
    parent_conns = []
    for i in range(num_workers):
        parent_conn, child_conn = Pipe()
        parent_conns.append(parent_conn)
        process = Process(target=download_worker, args=(num_per_worker, child_conn))
        process.start()

    # Collect data from all processes via pipes
    users = []
    for conn in parent_conns:
        users.extend(conn.recv())

    # Write it all to file
    with open(out_filename, 'wb') as out_file:
        headers = users[0].split('\n')[0]
        out_file.write(bytes(headers + '\n', 'UTF-8'))

        for user in users:
            user_info = user.split('\n')[1]
            out_file.write(bytes(user_info + '\n', 'UTF-8'))

def usage():
    print("""
    usage: python3 download_user_csv.py <num_users> <output file>
    ex: python3 download_user_csv.py 1000 out.csv
    """)

if __name__ == '__main__':
    if len(argv) != 3:
        usage()
        exit(0)

    n_users = int(argv[1])
    out_filename = argv[2]

    download_n_users(n_users, out_filename)

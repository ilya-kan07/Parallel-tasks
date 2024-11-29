import multiprocessing as mp
import time
import os
import argparse
from datetime import datetime
import csv

# py main.py -p 3 -f tasks.txt


def worker(child_conn):
    while True:
        task = child_conn.recv()

        if task == "exit":
            break

        task_name, task_time = task
        received_time = datetime.now().strftime('%H:%M:%S')

        time.sleep(task_time)
        completed_time = datetime.now().strftime('%H:%M:%S')

        child_conn.send(
            (task_name, os.getpid(), received_time, completed_time))
    child_conn.close()


def main(p, filename):
    tasks = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                name, time_required = line.rsplit(maxsplit=1)
                tasks.append((name, int(time_required)))
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
        return

    print(f"Loaded tasks: {tasks}")

    parent_connections = []
    processes = []

    for i in range(p):
        parent_conn, child_conn = mp.Pipe()
        process = mp.Process(target=worker, args=(child_conn,))
        process.start()
        parent_connections.append(parent_conn)
        processes.append(process)

    task_index = 0
    complete_index = 0
    completed_tasks = []

    for i in range(min(p, len(tasks))):
        parent_connections[i].send(tasks[task_index])
        task_index += 1

    while task_index < len(tasks) or complete_index < len(tasks):
        for i, conn in enumerate(parent_connections):
            if conn.poll():
                result = conn.recv()
                completed_tasks.append(result)
                complete_index += 1

                if task_index < len(tasks):
                    conn.send(tasks[task_index])
                    task_index += 1

    for conn in parent_connections:
        conn.send("exit")

    for process in processes:
        process.join()

    print("Writing results to output.csv")
    with open('output.csv', 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)

        writer.writerow(['Task Name', 'Process ID',
                        'Received Time', 'Completed Time'])

        for task in completed_tasks:
            writer.writerow(task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parallel task processing with pipes.")
    parser.add_argument("-p", type=int, required=True,
                        help="Number of processes.")
    parser.add_argument("-f", type=str, required=True, help="Input file name.")
    args = parser.parse_args()

    main(args.p, args.f)

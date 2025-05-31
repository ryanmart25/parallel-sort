#!/usr/bin/env python3
import sys
import subprocess

def main():
    # Expect exactly two args: output filename and an integer
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <output_filename> <integer>\nWhere output_filename is the prefix of the unsorted data files to be generated and integer is the number of records. The program will execute psort files with sizes 1000-><integer>. .")
        sys.exit(1)

    output_filename = sys.argv[1]
    integer_str = sys.argv[2]

    # Validate that the second argument is an integer
    try:
        int_value = int(integer_str)
    except ValueError:
        print("Error: the second argument must be an integer.")
        sys.exit(1)
    for i in range(1, int_value, 1000):
    # Build the command to run external.py using the same Python interpreter
        cmd = [
            "./psort",  # path to the current Python interpreter
            # add other arguments
            f"{output_filename}-{i}.bin", # input file
            f"sorted-{output_filename}-{i}.bin" # output file
        ]

    # Execute and stream output
        try:
            result = subprocess.run(
                cmd,
                check=True,
                text=True,          # decode stdout/stderr as text
                capture_output=False # streams directly to console
            )
        except subprocess.CalledProcessError as e:
            print(f"external.py exited with {e.returncode}", file=sys.stderr)
            sys.exit(e.returncode)

if __name__ == "__main__":
    main()


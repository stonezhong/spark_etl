import os

def get_filename(filename):
    return os.path.expandvars(os.path.expanduser(filename))

# read a text file
def read_textfile(filename):
    actual_filename = get_filename(filename)
    with open(actual_filename, "rt") as f:
        return f.read()


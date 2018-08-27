import os

def create_dir(filepath):
    if os.path.exists(filepath):
        print(filepath + ' already exists!')
        return filepath
    else:
        os.makedirs(filepath)
        print(filepath + ' folder created')

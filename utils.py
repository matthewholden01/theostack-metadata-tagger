import pickle
import re


def rename_file(filepath, suffix, extension):
    filename = str(filepath).split("/")[-1]
    regex = r"." + extension
    filename = re.sub(regex, suffix, filename)
    return filename


def storeData(filename, data):
    # filename = str(filepath).split('/')[-1]
    # filename = re.sub(r'\.pdf', '', filename)
    with open(filename, "wb") as file:
        pickle.dump(data, file)
        file.close()


def loadData(filename):
    # filename = str(filepath).split('/')[-1]
    # filename = re.sub(r'\.pdf', '', filename)
    with open(filename, "rb") as file:
        data = pickle.load(file)
        file.close()
    return data


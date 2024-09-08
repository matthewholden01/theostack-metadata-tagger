import os
import time

from unstructured.partition.pdf import partition_pdf
from langchain_core.documents import Document
from utils import loadData, rename_file, storeData


def process_document(filepath):
    origin_filename = filepath.split("/")[-1]
    processed_filepath = "ProcessedDocs/" + rename_file(origin_filename, "_elements", "pdf")
    elements = []
    try:
        elements = loadData(processed_filepath)
        print("Processed Document found. Returning processed elements. (TO DO: Add ability to reprocess)")
    except FileNotFoundError:
        print("Processing {0}".format(origin_filename))
        start = time.time()
        elements = partition_pdf(
            filepath,
            include_page_breaks=False,
            languages=["en"],
            strategy="auto",
            max_partition=None,
        )
        storeData(processed_filepath, elements)
        end = time.time()
        print("Finished processing in {0} seconds".format(end - start))
    return elements

def process_file(filepath):
    try:
        process_document(filepath)
    except Exception as e:
        with open("error_log.txt", "a") as f:
            f.write("Error processing document: " + filepath + "\n")
            f.write(str(e) + "\n")
        print("Error processing document: " + filepath)
        print(e)

def start(filepath):
    print("Theostack Document Procesor: Written by Matthew Holden")
    if os.path.isdir(filepath):
        print("Directory detected. Processing all files in directory.")
        process_directory(filepath)
    elif os.path.isfile(filepath):
        print("Single File detected. Processing file.")
        process_document(filepath)
    else:
        print("Invalid path")


def process_directory(dir_path):
    start = time.time()
    i = 1
    for file in os.listdir(dir_path):
        print("Progress: " + str(i) + " of " + str(len(os.listdir(dir_path))))
        filename = os.fsdecode(file)

        if filename.endswith(".pdf"):
            filepath = os.path.join(dir_path, filename)
            process_file(filepath)
        i += 1
    end = time.time()
    print("Finished Processing {0} files in {1} seconds".format(str(len(os.listdir(dir_path))), str(end - start))) 

dir_path = "/Users/holdem3/Documents/TheoStack/Docs/The Blood of the Covenant.pdf"

start(dir_path)
import os
import json
import pandas as pd
import pickle
import re
import numbers

from transformers import pipeline

from langchain_community.document_transformers.openai_functions import (create_metadata_tagger)
from langchain_community.cache import InMemoryCache
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.globals import set_llm_cache

from unstructured.partition.pdf import partition_pdf
from unstructured.documents.elements import Text
from unstructured.cleaners.core import group_broken_paragraphs

from openai import RateLimitError

accelerator = Accelerator()

def rename_file(filepath, suffix, extension):
  filename = str(filepath).split("/")[-1]
  regex = r"." + extension
  filename = re.sub(regex, suffix, filename)
  return filename

def storeData(filename, data):
  #filename = str(filepath).split('/')[-1]
  #filename = re.sub(r'\.pdf', '', filename)
  with open(filename, 'wb') as file:
    pickle.dump(data, file)

def loadData(filename):
  #filename = str(filepath).split('/')[-1]
  #filename = re.sub(r'\.pdf', '', filename)
  with open(filename, 'rb') as file:
    data = pickle.load(file)
  return data

def summarization(filepath, data):
  filename = rename_file(filename, "summarized", "pdf")
  #filename = str(filepath).split('/')[-1]
  #filename = re.sub(r'\.pdf', '_summarized', filename)
  try:
    return loadData(filename)
  except FileNotFoundError:
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
    summarized_pages = []
    combined_page = ""
    for page in data:
      if len(combined_page) + len(page) < 4000:
        combined_page += page
      else:
        summarized_pages.append(summarizer(combined_page, do_sample=False)[0]['summary_text'])
        combined_page = page
    storeData(filename, summarized_pages)
    return summarized_pages
    #with open(filename, "wb") as file:
   #   pickle.dump(summarized_pages,file)
    #return summarized_pages

def process_document(filepath):
  elements = []
  filename = rename_file(filepath, '_processed', 'pdf')
  print(filename)
  try:
    elements = loadData(filename)
    print("Recovering title from memory")
  except FileNotFoundError:
    print("File not found")
    elements = partition_pdf(filepath, include_page_breaks=False, languages=["en"], strategy="hi_res", max_partition=None)
    storeData(filename, elements)
  return elements


def makeRequestWithRetries(api_call, max_retries=5):
  for i in range(max_retries):
        try:
            return api_call()
        except RateLimitError:
            wait_time = (2 ** i) + random.random()
            time.sleep(wait_time)
  raise Exception("Still hitting rate limit after max retries")


if __name__ == "__main__":
  dir_path = '/Users/matthewholden/Documents/TheoStack/Docs/'
  xls = pd.ExcelFile("/Users/matthewholden/TheoBot/metadata-tagger/theostack ai metadata values.xlsx")

  sheets = ["V2 Tradition", "V2 Theology", "V2 Doctrine", "V2 Resource"]
  enum_names = ["Tradition", "Theology", "Doctrine", "Resource"]

  enums = {}
  for sheet, enum_name in zip(sheets, enum_names):
    df = pd.read_excel(xls, sheet)
    df_enums = []
    for  row in df.itertuples():
      if isinstance(row[1], numbers.Number):
        df_enums.append( "{0}: {1}".format(row[2], row[3]))
    enums[enum_name] = df_enums
  

  set_llm_cache(InMemoryCache())

  llm = ChatOpenAI(temperature=0.1, model="gpt-4o-mini", cache=True, max_retries=2)
  llm2 = ChatOpenAI(temperature=0.1, model="gpt-4")
  llm3 = ChatOpenAI(temperature=0.1, model="gpt-4o")
  llm_with_fallbacks = llm.with_fallbacks([llm2, llm3])
  
  labels = {}
  for file in os.listdir(dir_path):
    filename = os.fsdecode(file)
    
    if filename.endswith(".pdf"):
        print("Processing {filename}")
        filepath = os.path.join(dir_path, filename)

        pdf_elements = process_document(filepath)

        sections = []
        combined_text = ""
        narritive_text_started = False
        is_continuous = False
        for element in pdf_elements:
          if element.category == "Title":
            if narritive_text_started and not is_continuous:
              sections.append(combined_text)
              combined_text = ""
              narritive_text_started = False
            elif is_continuous:
              combined_text += " "
              combined_text += element.text
              combined_text += " "
       
              continue
            if len(element.text) > 1 and not is_continuous:
              combined_text += element.text
              combined_text += '\n'
            else:
              combined_text += element.text.rstrip()
          elif element.category == "NarrativeText":
            if not (str(element.text)[-2] in [".", "?", "!", ":"] or str(element.text)[-1] in [".", "?", "!", ":"]):
              is_continuous = True
            else:
              is_continuous = False
            combined_text += element.text
            narritive_text_started = True
        sections.append(combined_text)

        documents = [Document(page_content=ele, metadata={"word_count": len(ele)}) for ele in sections]

        schema = {
          "properties": {
              key: {"type": "string", "enum": value} for key, value in enums.items()
            },
            "required": enum_names,  
          } 

        document_transformer = create_metadata_tagger(llm=llm_with_fallbacks, metadata_schema=schema)
        
        metadata_filename = rename_file(filepath, '_metadata', 'pdf')

        try:
          enhanced_documents = loadData(metadata_filename)
          print('Loading Metadata from Memory')
        except FileNotFoundError:
          print('Document Not Found - processing with LLM')
          enhanced_documents = document_transformer.transform_documents(documents)
          storeData(metadata_filename, enhanced_documents)


        metadata_counter = {"Tradition": {}, "Theology": {}, "Doctrine": {}, "Resource": {}}

        for doc in enhanced_documents:
          for key, value in doc.metadata.items():
            if key in enum_names:
              tag = value.split(":")[0]
              try:
                metadata_counter[key][tag]["count"] += 1
                metadata_counter[key][tag]["word_count"] += doc.metadata["word_count"]
              except KeyError: 
                metadata_counter[key][tag] = { "count": 1, "word_count": doc.metadata["word_count"] }

        max_labels = []
        for key, value in metadata_counter.items():
          max_meta = ""
          max_count = 0
          max_words = 0
          for label, values in value.items():
            if values["count"] > max_count:
              max_meta = label
              max_count = values["count"]
              max_words = values["word_count"]
            elif values["count"] == max_count:
              if values["word_count"] > max_words:
                max_words = values["word_count"]
                max_meta = label
          max_labels.append({key: max_meta})
        print(max_labels)
          
    else:
        continue
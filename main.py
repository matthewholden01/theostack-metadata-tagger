import os
from accelerate import Accelerator
import json

from langchain_community.document_transformers.openai_functions import (create_metadata_tagger)
from langchain_community.cache import InMemoryCache
from langchain_unstructured import UnstructuredLoader
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.globals import set_llm_cache
import pickle
from unstructured.partition.pdf import partition_pdf
from unstructured.documents.elements import Text
from unstructured.cleaners.core import group_broken_paragraphs
import re
from transformers import pipeline
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


def schema_creator(): 

  traditions =[
 "Reformed Baptist: A branch of Baptists adhering to Reformed theology, emphasizing doctrines like predestination and God's sovereignty, often following the 1689 Baptist Confession of Faith.",
 "Presbyterian: A Protestant denomination governed by a system of elders (presbyters) and adhering to Reformed theology, notably the Westminster Confession of Faith.",
 "Dutch Reformed: A Reformed tradition originating in the Netherlands, known for its adherence to the Heidelberg Catechism and the Canons of Dort.",
 "Reformed Anglican: A tradition within Anglicanism that upholds Reformed theology, often adhering to the Thirty-Nine Articles and the Book of Common Prayer.",
 "Neo-Calvinist: A modern movement within Reformed Christianity emphasizing the application of Christian principles to all areas of life, influenced by thinkers like Abraham Kuyper.",
 "Puritan: A historical movement within English Protestantism seeking to purify the Church of England from within, emphasizing personal piety and adherence to Scripture.",
 "Misc. Reformed Protestants: A category encompassing various smaller Reformed denominations that hold to similar confessions and creeds as the larger Reformed tradition.",
 "Lutheran: A major Protestant tradition founded on the teachings of Martin Luther, emphasizing justification by faith alone and adhering to the Augsburg Confession.",
 "Anglican: A tradition rooted in the Church of England, combining elements of Reformation theology with traditional liturgy, governed by the Book of Common Prayer and the Thirty-Nine Articles.",
 "Methodist: A Protestant denomination originating from the revival movement of John Wesley, emphasizing personal holiness and social justice, often adhering to the Articles of Religion.",
 "Non-Reformed Baptist: A branch of Baptists that may not adhere to Reformed theology, often emphasizing believer's baptism and congregational governance.",
 "Mennonite and Amish: An Anabaptist tradition emphasizing pacifism, simple living, and community, with the Amish being a more conservative branch.",
 "Non-Denominational: Churches or groups not formally affiliated with any specific denomination, often emphasizing independent governance and a focus on the Bible as the sole authority.",
 "Plymouth Bretheren: A conservative evangelical movement emphasizing the priesthood of all believers, simple worship, and the expectation of Christ's imminent return.",
 "Pentecostalism: A Protestant movement emphasizing the work of the Holy Spirit, spiritual gifts, and revivalism, often adhering to a belief in the necessity of a second baptism of the Holy Spirit.",
 "Vineyard Churches: A denomination within the charismatic movement, emphasizing contemporary worship, the gifts of the Spirit, and a commitment to both evangelism and social justice.",
 "Congregationalist: A tradition that emphasizes the autonomy of the local church, governed by the congregation, often adhering to Reformed theology.",
 "Evangelical: A broad movement across various denominations emphasizing the authority of Scripture, the necessity of personal conversion, and active evangelism.",
 "Baptist (General): A broad Protestant tradition emphasizing believer's baptism, congregational governance, and the authority of Scripture.",
 "Southern Baptist: The largest Baptist denomination in the United States, emphasizing evangelical beliefs, believer's baptism, and conservative social values.",
 "Seventh-day Adventist: A Protestant denomination emphasizing the observance of Saturday as the Sabbath, the imminent return of Christ, and a holistic view of health and lifestyle.",
 "Quaker (Religious Society of Friends): A Protestant movement emphasizing direct personal experience of God, simplicity, pacifism, and communal decision-making.",
 "Church of Christ: A Protestant denomination that emphasizes restorationism, seeking to return to the practices of the early church, with a focus on baptism and weekly communion.",
 "Church of Nazarene: A Protestant denomination within the Wesleyan-Holiness tradition, emphasizing sanctification, evangelical outreach, and social holiness.",
 "Wesleyan Church: A Protestant denomination that follows the teachings of John Wesley, emphasizing holiness, social justice, and evangelism.",
 "Advent Christian Church: A Protestant denomination emphasizing the imminent return of Christ and the conditional immortality of the soul.",
 "African Methodist Episcopal Church: A historically Black Protestant denomination emphasizing social justice, liberation theology, and Methodism within the African American community.",
 "Churches of Christ (Restoration Movement): A Protestant denomination emphasizing the restoration of New Testament Christianity, with a focus on baptism and weekly observance of the Lord's Supper.",
 "Assemblies of God: A Pentecostal denomination emphasizing the work of the Holy Spirit, spiritual gifts, and evangelism, with a strong belief in the baptism in the Holy Spirit.",
 "Christian and Missionary Alliance: A Protestant denomination emphasizing evangelism, missions, and the deeper Christian life, with a focus on Christ as Savior, Sanctifier, Healer, and Coming King.",
 "United Church of Christ: A mainline Protestant denomination known for its progressive stance on social issues, emphasizing the autonomy of local congregations and a commitment to justice.",
 "Free Will Baptist: A Baptist tradition emphasizing free will in salvation, believer's baptism, and congregational governance.",
 "International Church of the Foursquare Gospel: A Pentecostal denomination emphasizing the fourfold ministry of Jesus as Savior, Baptizer with the Holy Spirit, Healer, and Soon-Coming King.",
 "Salvation Army: A Protestant denomination and charitable organization emphasizing evangelism, social service, and holiness, known for its military-style structure and uniform."
 ]

  schema = {
    "properties": {
        "tradition": {"type": "string", "enum": traditions},
        
    },
    "required": ["tradition"],  
  }
  #prompt = (
  #  f"Given the following text, determine which Christian tradition it most closely aligns with."
  #  f"Use the following options: {', '.join(tradition_list)}."
  #  f"Text:\n{document_text}\n"
  #  "Please return the author, text title, and the most appropriate tradition."
  #)

  
  #response = client.chat.completions.create(
  #  model="gpt-4o-mini",
  #  messages=[
  #    {"role": "system", "content": "You are an expert in Christian Theology"},
  #    {"role": "user", "content": prompt}
  #  ]
  #)

  return schema

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

  set_llm_cache(InMemoryCache())

  llm = ChatOpenAI(temperature=0.1, model="gpt-4o-mini", cache=True, max_retries=2)
  llm2 = ChatOpenAI(temperature=0.1, model="gpt-4")
  llm3 = ChatOpenAI(temperature=0.1, model="gpt-4o")
  llm_with_fallbacks = llm.with_fallbacks([llm2, llm3])
  
  labels = {}
  for file in os.listdir(dir_path):
    filename = os.fsdecode(file)
    
    if filename.endswith(".pdf"):
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

        schema = schema_creator()
        document_transformer = create_metadata_tagger(llm=llm_with_fallbacks, metadata_schema=schema)

        metadata_filename = rename_file(filepath, '_metadata', 'pdf')

        try:
          enhanced_documents = loadData(metadata_filename)
          print('Loading Metadata from Memory')
        except FileNotFoundError:
          print('Document Not Found - processing with LLM')
          enhanced_documents = document_transformer.transform_documents(documents)
          storeData(metadata_filename, enhanced_docs)



        metadata_counter = {"tradition": {}}

        for doc in enhanced_documents:
          for key, value in doc.metadata.items():
            if key == "tradition":
              tag = value.split(":")[0]
              try:
                metadata_counter[key][tag]["count"] += 1
                metadata_counter[key][tag]["word_count"] += doc.metadata["word_count"]
              except KeyError: 
                metadata_counter[key][tag] = { "count": 1, "word_count": doc.metadata["word_count"] }

        print(metadata_counter)
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
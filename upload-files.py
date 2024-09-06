docs = [{"tradition": "Misc. Reformed Protestants", "word_count": 100},{"tradition": "Reformed Baptist: A branch of Baptists adhering to Reformed theology, emphasizing doctrines like predestination and God's sovereignty, often following the 1689 Baptist Confession of Faith.", "word_count": 200},{"tradition": "Evangelical", "word_count": 10},{"tradition": "Misc. Reformed Protestants", "word_count": 30}, {"tradition": "Evangelical", "word_count": 100}, {"tradition": "Misc. Reformed Protestants", "word_count": 100}, {"tradition": "Evangelical", "word_count": 39}, {"tradition": "Evangelical", "word_count": 130}, {"tradition": "Misc. Reformed Protestants", "word_count": 123}, {"tradition": "Misc. Reformed Protestants", "word_count": 12}, {"tradition": "Evangelical", "word_count": 83}, {"tradition": "Misc. Reformed Protestants", "word_count": 34}]

metadata_counter = {"tradition": {}}

for doc in docs:
  for key, value in doc.items():
    if key == "tradition":
      tag = value.split(":")[0]
      try:
        metadata_counter[key][tag]["count"] += 1
        metadata_counter[key][tag]["word_count"] += doc["word_count"]
      except KeyError: 
        metadata_counter[key][tag] = { "count": 1, "word_count": doc["word_count"] }


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
import os

from langchain_community.document_transformers.openai_functions import (create_metadata_tagger)
from langchain_unstructured import UnstructuredLoader
from langchain_core.documents import Document
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

schema = {
    "properties": {
        "document_title": {"type": "string"},
        "author": {"type": "string"},
        "tradition": {"type": "string", "enum": ["Reformed Baptist: A branch of Baptists adhering to Reformed theology, emphasizing doctrines like predestination and God's sovereignty, often following the 1689 Baptist Confession of Faith.",
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
                                                  "Salvation Army: A Protestant denomination and charitable organization emphasizing evangelism, social service, and holiness, known for its military-style structure and uniform."]
                                        },    
    },
    "required": ["tradition", "document_title", "author"]  

}

dir_path = '/Users/matthewholden/Documents/'
filename = os.path.join(dir_path, 'CHS_All of Grace.pdf')

loader = UnstructuredLoader(file_path=filename, chunking_strategy="basic", max_characters=1000000000)
docs = loader.load()

print(len(docs))


prompt = ChatPromptTemplate.from_template(
    """Extract the tradition of the document. The tradition is a Christian denomination or movement.
    Pick the tradition from one of the given tradition types. The tradition is given as a string followed by a colon. After the colon
    is a description of the tradition which you should use to help classify the tradition of the document. For the tradition, only return
    the word given before the colon and not the description.

    {input}

    You should also extract the author and the title of the document. The author is the person who wrote the document. The title is the name of the document.
   """
   )

llm = ChatOpenAI(temperature=0, model="gpt-4o-mini")
document_transformer = create_metadata_tagger(metadata_schema=schema, llm=llm, prompt=prompt)

enhanced_docs = document_transformer.transform_documents(docs)

import json

print(*[json.dumps(d.metadata for d in enhanced_docs)], sep="\n\n---------------\n\n")
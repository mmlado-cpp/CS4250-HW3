#-------------------------------------------------------------------------
# AUTHOR: Martin Lado
# FILENAME: db_connection_mongo_solution.py
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: 2:30
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import string
from pymongo import MongoClient

def connectDataBase():
    # Create a database connection object using pymongo
    # --> add your Python code here
    client = MongoClient('mongodb://localhost:27017/')
    db = client['Assignment3']
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    docText = docText.translate(str.maketrans('', '', string.punctuation)).lower().split()
    term_count = {}
    for term in docText:
        term_count[term] = term_count.get(term, 0) + 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    terms = [{"term": term, "count": count, "num_chars": len(term)} for term, count in term_count.items()]


    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "terms": terms
    }
    
    # insert the document
    # --> add your Python code here
    col.insert_one(document)
    
def deleteDocument(col, docId):
    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})


def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)
    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = {}
    cur = col.find({})
    for doc in cur:
        title = doc['title']
        for term_obj in doc['terms']:
            term = term_obj['term']
            count = term_obj['count']
            if term not in index:
                index[term] = {}
            if title in index[term]:
                index[term][title] += count
            else:
                index[term][title] = count
                
    formatted_index = {term: ', '.join([f'{title}:{count}' for title, count in sorted(doc_counts.items())]) for term, doc_counts in index.items()}
    sorted_index = {term: formatted_index[term] for term in sorted(formatted_index.keys())}
    return sorted_index
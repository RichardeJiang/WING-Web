import sqlite3
from sqlite3 import OperationalError
from operator import itemgetter
import mysql.connector as mc
import pymysql

def writeTexFile(content, fileName):
    fp = open(fileName, "w")
    for cell in content:
        fp.write(cell)
    fp.close()
    return

if __name__ == "__main__":

    conn = mc.connect(user='root', password='',
                              host='127.0.0.1',
                              port='3307',
                              database='wing')
    # conn = sqlite3.connect('wing')
    # conn = mc.connect('test')
    c = conn.cursor()

    # Open and read the file as a single buffer
    # fd = open('pub.sql', 'r')
    # sqlFile = fd.read()
    # fd.close()

    # # all SQL commands (split on ';')
    # sqlCommands = sqlFile.split(';')

    # # Execute every command from the input file
    # for command in sqlCommands:
    #     # This will skip and report errors
    #     # For example, if the tables do not yet exist, this will skip over
    #     # the DROP TABLE commands
    #     try:
    #         c.execute(command)
    #     except OperationalError, msg:
    #         print "Command skipped: ", msg

    listToWrite = []

    publicationResult = c.execute("SELECT id, pubtype, url, title, year FROM jos_jresearch_publication;")
    # publicationResult = c.execute("SELECT * FROM jos_jresearch_publication;")
    # publicationResult = c.execute("SELECT * FROM jos_banner;")
    print "ok\n"
    # print publicationResult
    # publicationResult = c.execute("SELECT id, pubtype, url, title, year FROM jos_banner;")
    publications = c.fetchall()

    for publication in publications:
        pubId = publication[0]
        pubtype = publication[1]
        url = publication[2]
        title = publication[3]
        year = publication[4]
        authorResult = c.execute("SELECT author_name, author_order FROM jos_jresearch_publication_external_author WHERE id_publication=" + str(pubId) + ";")
        authors = c.fetchall()
        authorInOrder = sorted(authors, key=itemgetter(1))
        print authorInOrder
        # authorInOrder = [(ele[0].decode('iso-8859-1').encode('utf-8'), ele[1]) for ele in authorInOrder]
        authorList = " and ".join([str(ele[0].encode('utf-8')) for ele in authorInOrder])
        content = "@" + str(pubtype) + "{\n"
        content += ("author = " + "\"" + str(authorList) + "\",\n")
        content += ("title = " + "\"" + str(title.encode('utf-8')) + "\",\n")
        
        if str(pubtype) == "conference":
            conferenceResult = c.execute("SELECT booktitle FROM jos_jresearch_conference WHERE id_publication = " + str(pubId) + ";")
            conference = str(c.fetchall()[0][0])
            content += ("booktitle = \"" + conference + "\",\n")
        elif str(pubtype) == "article":
            articleResult = c.execute("SELECT journal, volume, pages FROM jos_jresearch_article WHERE id_publication = " + str(pubId) + ";")
            article = c.fetchall()[0]
            journal = str(article[0])
            volume = str(article[1])
            pages = str(article[2])
            content += ("journal = \"" + journal + "\",\n")
            content += ("volume = \"" + volume + "\",\n")
            content += ("pages = \"" + pages + "\",\n")
        elif str(pubtype) == "phdthesis":
            schoolResult = c.execute("SELECT school FROM jos_jresearch_phdthesis WHERE id_publication = " + str(pubId) + ";")
            school = str(c.fetchall()[0][0])
            content += ("school = \"" + school + "\",\n")
        elif str(pubtype) == "mastersthesis":
            schoolResult = c.execute("SELECT school FROM jos_jresearch_mastersthesis WHERE id_publication = " + str(pubId) + ";")
            school = str(c.fetchall()[0][0])
            content += ("school = \"" + school + "\",\n")

        content += ("year = " + str(year) + "}\n\n")

        listToWrite.append(content)

    writeTexFile(listToWrite, "actual.bib")

    # For each of the 3 tables, query the database and print the contents
    # for table in ['ZooKeeper', 'Animal', 'Handles']:


    #     **# Plug in the name of the table into SELECT * query
    #     result = c.execute("SELECT * FROM %s;" % table);**

    #     # Get all rows.
    #     rows = result.fetchall();

    #     # \n represents an end-of-line
    #     print "\n--- TABLE ", table, "\n"

    #     # This will print the name of the columns, padding each name up
    #     # to 22 characters. Note that comma at the end prevents new lines
    #     for desc in result.description:
    #         print desc[0].rjust(22, ' '),

    #     # End the line with column names
    #     print ""
    #     for row in rows:
    #         for value in row:
    #             # Print each value, padding it up with ' ' to 22 characters on the right
    #             print str(value).rjust(22, ' '),
    #         # End the values from the row
    #         print ""

    c.close()
    conn.close()
    pass
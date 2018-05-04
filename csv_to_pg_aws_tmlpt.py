
"""This program inserts customer data from a csv file into a postgres database for Control-M usage statistics"""

#Libs required for the database interface

import csv
import psycopg2 as pg2
import os
import os.path
import sys


newpath = 'c:\ctm_reports\input\\'
newfile = 'c:\ctm_reports\input\\abc.csv'                    #sys.argv[1]
procfile = newpath + newfile
mypath = 'c:\\ctm_reports\ctmoutput\\'
#checkfile = sys.argv[1]
#myinput = mypath + checkfile

#print(myinput)
#print(procfile)
#print (newfile)
#try:
conn = pg2.connect("dbname='mydb' user='myuser' host='myhost.compute.amazonaws.com' password='mypass'")
cursor = conn.cursor()


#def duplicate():        #Checks to see if filename has already been processed

 #   if os.path.isfile(myinput+'.txt'):
 #       print ("The file exists - Please be careful of duplicate entries in the database")
 #       sys.exit(1)

#duplicate()


reader = csv.reader(open(newfile,'rU'))     #read csv file and then inserts to PG
for row in reader:
    print (row[1])

    statement = "INSERT INTO ingest (name,surname,address,partner) VALUES ('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "')"

    cursor.execute(statement)
    conn.commit()

#f = open(mypath + checkfile +'.txt','w')
#f.write(newfile + ' insert successful\n') # python will convert \n to os.linesep
#f.close()



#    templateid = cursor.fetchone()[1]

#   statement = "INSERT INTO usage_input "



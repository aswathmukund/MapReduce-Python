#!/usr/bin/env python

' Simple Join operation in Map Reduce - Python streaming'
' Student table has  ID,Name'
' Marks table has ID,Marks'
' For every ID, sum all the marks and display only if the Student has atleast one mark'

import sys
import csv

# input comes from STDIN (standard input)

def mapper1(args):


    ''' using csv reader, change the number of arguments

        incon = open(table, 'rU')
        read = csv.reader(incon)
        for every in read:
            print every[0],"Student",every[1]

        for line in sys.stdin:
            line = line.strip()
            uid, item = line.split(",")
            print uid,"Marks",item
    '''

    ''' In the mapper step, tag each row with the name of the table '''

    for line in sys.stdin:
        line = line.strip()
        uid, item = line.split(",")
            
        if item.isdigit():
            print uid,"Marks",item

        else:
            print uid,"Student",item


def reducer1(args):
    current_key = None
    marksum = 0
    name = []
    for line in sys.stdin:
        line = line.strip()
        key,table,value= line.split(' ')
        
        if current_key is not None and current_key!=key:
            if len(name) == 0:
                name2 = "Unknown"
            else:
                name2 = name[0]
                
            'taking the first value from the name [] list - Just incase if there are two entries for the same ID'

            if name2 != "Unknown" and marksum != 0 :
                print current_key,name2,marksum
                marksum = 0
                name = []
         
        
        current_key = key
        if table == "Student":
            name = name + [value]

        if  table == "Marks":
            marksum = marksum + int(value)

    if len(name) == 0:
        name2 = "Unknown"
    else:
        name2 = name[0]
                
    'taking the first value from the name [] list - Just incase if there are two entries for the same ID'

    if name2 != "Unknown" and marksum != 0 :
        print current_key,name2,marksum  
    




if __name__ == "__main__":
    if sys.argv[1] == "mapper1":
        mapper1(sys.argv[2:])
        #mapper1(sys.argv[2],sys.argv[3:])
    if sys.argv[1] == "reducer1":
        reducer1(sys.argv[2:])
        

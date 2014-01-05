'Simple Join operation in Map Reduce using MRJob'
' Student table has  ID,Name'
' Marks table has ID,Marks'
' For every ID, sum all the marks and display only if the Student has atleast one mark'

from mrjob.job import MRJob
class MRjoin(MRJob):
   
    def mapper(self, key, line):
        uid, item = line.split(",")
        
        if item.isdigit():
            yield uid,("Marks",item)

        else:
            yield uid, ("Student",item)


    def reducer(self, user, values):

     
        marks = 0
        name = []

        for every in values:
            if every[0] == "Student":
                name = name + [every[1]]
                
                
            if every[0] == "Marks":
                marks = marks+ int(every[1])

            if len(name) == 0:
                name2 = "Unknown"
            else:
                name2 = name[0]
                

        
        'Edit this for other types of joins like outer or full'
        if name2 != "Unknown" and marks != "" :
            yield(user,(name2,marks))     
          
          
            
                

if __name__ == '__main__':
     MRjoin.run()
     


    
   

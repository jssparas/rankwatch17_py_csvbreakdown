'''
Created on 30-Oct-2017

@author: linux
'''

from time import sleep
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import csv

raw_directory = "/home/linux/raw/"
processed_directory = "/home/linux/processed/"


# "function to create processed csv file and write the data provided as argument"
def create_processed_csv_file(row):
    with open(processed_directory + row[0] + "-processed.csv", "w") as output_csv_file:
        writer = csv.writer(output_csv_file, delimiter=',' )
        writer.writerow(row[1:])
        return output_csv_file
    
# "handler to handle file created event using PatternMatchingEventHandler"
class handler(PatternMatchingEventHandler):
    
    pattern = ["*.csv"]
    
    def process_file(self, event):
        print event.event_type
        if event.event_type == "created":
            with open(event._src_path, "r") as input_csv_file:
                reader = csv.reader(input_csv_file, delimiter=',')
                for row in reader:
                    print "processed file created -- %s" % create_processed_csv_file(row) 
            
        
    def on_created(self, event):
        self.process_file(event)
        

# "created observer, attached handler and started it to listen for events and passed it to handler"
if __name__ == "__main__":
    observer = Observer();
    observer.schedule(handler(), path=raw_directory, recursive=False)
    observer.start()
    
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        
    observer.join()
    
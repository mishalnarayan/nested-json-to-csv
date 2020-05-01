import sys
import json
import csv
reload(sys)
sys.setdefaultencoding('utf8') 
import os
import pandas as pd 
import time
max_field_length = 0
import csv
import glob
from datetime import datetime
start_time = datetime.now()
print start_time
import json
import ast

#allfiless = glob.glob("*.json")
#allfiles = [""]


#Recursive file listing function start

class FP:

    def __init__(self, path):
        self.path = path
        self.raw_data = []

    def collectAllDataFiles(self, path):
        """
        This function will create the data directory dictionary
        param path: Path of the dataset
        return: None
        """

        directory = [
                     os.path.join(
                                  path,
                                  filename
                     ) for filename in os.listdir(path) if 'docs' not in filename
        ]

        for file in directory:
            if os.path.isdir(file):
                #print 'Inside '+str(file)
                self.collectAllDataFiles(file)
            else:
                self.raw_data.append(file)

    def returnDd(self):
        return self.raw_data
#Recursive file listing function end



o = FP('/ujju/data/path_to_json_dataset')

o.collectAllDataFiles('/ujju/data/path_to_json_dataset') #provide path to your json data folder

allfiless = o.returnDd()

# #filtering json files
file_count = 0
allfiles = []
for single in allfiless :
    if str(single).endswith(".json") and float(os.path.getsize(single)) > 0 :
        allfiles.append(single)
        print single
    else :
        print single
        print float(os.path.getsize(single))/(1024.0*1024.0)





def to_string(s):
    try:
        return str(s)
		#return s.encode('utf-8', errors = "replace").strip()
    except:
		#pass
        #Change the encoding type if needed
        return s.encode('utf-8')
		#return s.encode('ascii', errors = 'replace').strip()

#This is where magic happens
def reduce_item(key, value):
    global reduced_item
    
    #if to be iterate is list
    if isinstance(value ,list):
        #print "list loop got execute"
        i=0
        for sub_item in value:
            reduce_item(key+'___'+to_string(i), sub_item)
            i=i+1

    #if to be iterate is dictionary
    elif isinstance(value, dict):
        #print "dict loop got executed "
        sub_keys = value.keys()
        for sub_key in sub_keys:
            reduce_item(key+'___'+to_string(sub_key), value[sub_key])
    
    #Final data extraction
    else:
        #reduced_header_list.append(str(key)  + "_________" + to_string(value))
		current_reduced_header_dictionary[str(key)] = to_string(str(value))
		




key_list = []


#ultimate_data_list = []	

#memory issue ugly fix	


import gzip

for file in allfiles :
    key_list = []
    print file
    writing_file = "***********************++++++++++++++++++++************************* " + str(file) + " **************************+++++++++++++++++++****************************"
    print writing_file
    ultimate_data_list = open("/ujju/data/a_temporary_file.txt", "w") #opening file for writing metadata(converting json data lines to flat dictionary)


    count = 1
    line_count = 0
    
    #fp = gzip.open(file, 'r').readlines()
    fp = open(file, 'r').readlines()
    
    for single_json_line in fp :
        try :

            if len(single_json_line) < 5 :
                pass
            else :
                single_json_line = single_json_line

                try :

                    value = json.load(single_json_line)
                except :
                    value = json.loads(single_json_line)
                key = ""
                current_reduced_header_dictionary = {}
                reduce_item(key, value)
                #key_list += current_reduced_header_dictionary.keys()
                for x in current_reduced_header_dictionary.keys() :
                    key_list.append(x)

                #print "No of headers in current line : "  + str(len(current_reduced_header_dictionary.keys()))
                key_list = set(key_list)
                key_list = list(key_list)


                #ultimate_data_list.append(current_reduced_header_dictionary)
                #print current_reduced_header_dictionary
                ultimate_data_list.write(str(current_reduced_header_dictionary) + "\n")
                count = count + 1  
                #print "currently working on line no : " + str(count) 
        except :
            count = count + 1 
            print count
            pass

    file_count = file_count + 1
    percent_calci = (float(file_count)/float(len(allfiles)))*100.0
    print "working on file no " + str(file_count) + " of total file " + str(len(allfiles)) + " " + str(percent_calci) + " % completed"        

#ignore ugly fixes
    ultimate_data_list.close()



    ultimate_data_list = open("/ujju/data/a_temporary_file.txt", "r") #Reading metatdata file





    o1 = open("/ujju/data/path_for_output_files/" + (str(((str(file)).split('/'))[-1])).replace(".json",".csv").replace("$","_").replace(".gz",""), "a") #output file path
    f1 = csv.writer(o1)

    ##Converting metadata to flat files

    #writing header of file
    temporary_writing_list = []
    for single_key in key_list :
        temporary_writing_list.append(single_key)
    f1.writerow(temporary_writing_list)



    for single_line_data in ultimate_data_list :
        single_line_data = ast.literal_eval(single_line_data)
        
        #print single_line_data.keys()

        temporary_writing_list = []
        for single_key in key_list :
            try :

                temporary_writing_list.append(single_line_data[single_key])

            except :
                temporary_writing_list.append("") # add here in the braces"This tag doesn't exist in this line"

        f1.writerow(temporary_writing_list)

    o1.close()
end_time = datetime.now()   
print('Duration: {}'.format(end_time - start_time))



#just a merger if there are multiple files with same json structure and neede to be merged together

import sys
import json
import csv
reload(sys)
sys.setdefaultencoding('utf8') 
import os
import pandas as pd 
import time
max_field_length = 0
import csv
import glob
from datetime import datetime
start_time = datetime.now()
print start_time
import json
import ast



chunksize = 100

allfiles = glob.glob("/ujju/data/path_for_output_files/*.csv")

#deciding original column
for file_path in allfiles :
    print file_path

    column_deciding_dataframe = pd.read_csv(file_path,nrows = 1)

    unique_column = []

    # for column_s in column_deciding_dataframe.columns :
    #     column_s = ''.join(i for i in column_s if not i.isdigit())
    #     unique_column.append(column_s)
    #     unique_column = set(unique_column)
    #     unique_column = list(unique_column)

    for column_s in column_deciding_dataframe :
        column_s = str(((str(column_s)).split('/'))[-1])
        new_list = []
        for x in column_s.split("__") :
            if len(x) == 0 :
                pass
            else :           
                try:
                    m = int(x)
                    if len(str(m)) < 0 :
                        raise
                except :
                    new_list.append(x)
        column_s = "__"+"_".join(new_list)# + "___"
        print column_s
        #column_s = ''.join(i for i in column_s if not i.isdigit())[:-3]
        unique_column.append(column_s)
        unique_column = set(unique_column)
        unique_column = list(unique_column)


    print unique_column
    for df in pd.read_csv(file_path, chunksize = chunksize,sep = ",") :
        for single_unique_column in unique_column :
            for duplicated_column in df.columns :

                #duplicated_column_uniqued = ''.join(i for i in duplicated_column if not i.isdigit())
                new_list = []
                for x in duplicated_column.split("__") :
                    if len(x) == 0 :
                        pass
                    else :           
                        try:
                            m = int(x)
                            if len(str(m)) < 0 :
                                raise
                        except :
                            new_list.append(x)
                duplicated_column_uniqued = "__"+"_".join(new_list)# + "___"

                if duplicated_column_uniqued == single_unique_column :
                    extracted_frame = df[[duplicated_column]]

                    #extracted_frame["date"] = str(''.join(i for i in file_path.split("/")[-1] if i.isdigit()))
                    #print extracted_frame
                    if os.path.isfile("/ujju/data/path_for_output_files/cleaned/" + single_unique_column.replace("{","").replace("}","").replace(":","").replace("/","") + str(".csv")) :
                        with open("/ujju/data/path_for_output_files/cleaned/" + single_unique_column.replace("{","").replace("}","").replace(":","").replace("/","") + str(".csv"), "a" ) as  f :
                            extracted_frame.to_csv(f, index = False, header = False)
                    else :
                        with open("/ujju/data/path_for_output_files/cleaned/" + single_unique_column.replace("{","").replace("}","").replace(":","").replace("/","") + str(".csv"), "a" ) as  f :
                            f1 = csv.writer(f)
                            f1.writerow([single_unique_column])
                            extracted_frame.to_csv(f, index = False, header = False)

end_time = datetime.now()   
print('Duration: {}'.format(end_time - start_time))

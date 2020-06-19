import sqlite3
import json
import ast
import pandas
import os
import time
from datetime import datetime
#Huiyun Peng
#10 Mar 2020

#running noworkflow command: python3 -m noworkflow run test5.py

#data is stored in input_db_file
#call cursor() to create an object of it and use its execute() method to perform SQL


#complex files
# input_db_file = '/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
# run_num = 7

#simple ones
input_db_file = '/Users/huiyunpeng/Desktop/.noworkflow/db.sqlite'
run_num = 7

db = sqlite3.connect(input_db_file, uri=True)
c = db.cursor()

def get_name(code_component_id):
    '''
    get name from code_component_id in code_component table
    '''
    id_name_pair = {}
    c.execute('SELECT id, name from code_component where trial_id = ?', (run_num,))
    i=1
    for row in c:
        for char in row:
            if (isinstance(char, str)):
                id_name_pair[i] = char
                i = i+1
    return id_name_pair.get(code_component_id)

def get_line_col_info(code_component_id, item):
    temp = []
    if (item == "last_char_column"):
        c.execute('SELECT id, last_char_column from code_component where trial_id = ?', (run_num,))
    elif (item == "last_char_line"):
        c.execute('SELECT id, last_char_line from code_component where trial_id = ?', (run_num,))
    elif (item == "first_char_column"):
        c.execute('SELECT id, first_char_column from code_component where trial_id = ?', (run_num,))
    elif (item == "first_char_line"):
        c.execute('SELECT id, first_char_line from code_component where trial_id = ?', (run_num,))
    for row in c:
        for char in row:
            temp.append(char)

    id_line_pair = {}
    a=1
    #loop through index
    for i in range(len(temp)):
        if (i % 2 != 0):
            id_line_pair[a] = temp[i]
            a = a+1
    return id_line_pair.get(code_component_id)

def get_type(code_component_id):
    '''
    get type from code_component_id in code_component table
    '''
    id_type_pair = {}
    c.execute('SELECT id, type from code_component where trial_id = ?', (run_num,))
    i=1
    for row in c:
        for char in row:
            if (isinstance(char, str)):
                id_type_pair[i] = char
                i = i+1
    return id_type_pair.get(code_component_id)

def get_mode(code_component_id):
    '''
    get mode from code_component_id in code_component table
    '''
    id_mode_pair = {}
    c.execute('SELECT id, mode from code_component where trial_id = ?', (run_num,))
    i=1
    for row in c:
        for char in row:
            if (isinstance(char, str)):
                if (char == "r"):
                    id_mode_pair[i] = "read"
                elif (char == "w"):
                    id_mode_pair[i] = "written"
                elif (char == "d"):
                    id_mode_pair[i] = "deleted"
                else:
                    id_mode_pair[i] = char
                i = i+1
    return id_mode_pair.get(code_component_id)

id = []
result = []
def get_top_level_component_id():
    '''
    returns a list of top-level code_component
    '''
    c.execute('SELECT id from code_component where trial_id = ?', (run_num,))
    #get all id
    for row in c:
        for char in row:
            id.append(char)
    #select the top_level_component id
    top_level_component_id = []
    #for code component with single lines
    line_num = 0
    for element in id:
        if (get_type(element) != "module"):
            #skip the first line
            if (get_type(element) == "script"):
                top_level_component_id.append(element)
            else:
                if (line_num != get_line_col_info(element, "first_char_line")):
                    top_level_component_id.append(element)
                    line_num = get_line_col_info(element, "first_char_line")


    #for code component with mutiple lines
    #if it is in a single line and line_num <= last_line_num: remove
    last_line_num = 0

    #old version, more fine-grained
    # for element in top_level_component_id:
    #     if (get_type(element) == "script" or get_type(element) == "function_def" ):
    #         result.append(element)
    #     else:
    #         if (get_last_line_num(element) == get_line_num(element)):
    #             if (get_line_num(element) > last_line_num):
    #                 result.append(element)
    #                 last_line_num = get_last_line_num(element)
    #         else: # add to result, update last_line_num
    #             result.append(element)
    #             last_line_num = get_last_line_num(element)

    for element in top_level_component_id:
        if (get_type(element) == "script"):
            result.append(element)
        else:
            if (get_line_col_info(element, "first_char_line") > last_line_num):
                result.append(element)
                last_line_num = get_line_col_info(element, "last_char_line")
    return result


# id = []
# result = []
# def get_top_level_component_id():
#     '''
#     returns a list of top-level code_component
#     '''
#     c.execute('SELECT id from code_component where trial_id = ?', (run_num,))
#     #get all id
#     for row in c:
#         for char in row:
#             id.append(char)
#     #select the top_level_component id
#     top_level_component_id = []
#     #for code component with single lines
#     line_num = 0
#     for element in id:
#         if (get_type(element) != "module"):
#             #skip the first line
#             if (get_type(element) == "script"):
#                 top_level_component_id.append(element)
#             else:
#                 if (line_num < get_line_num(element)):
#                     top_level_component_id.append(element)
#                     line_num = get_line_num(element)


#     #for code component with mutiple lines
#     #if it is in a single line and line_num <= last_line_num: remove
#     last_line_num = 0

#     #old version, more fine-grained
#     # for element in top_level_component_id:
#     #     if (get_type(element) == "script" or get_type(element) == "function_def" ):
#     #         result.append(element)
#     #     else:
#     #         if (get_last_line_num(element) == get_line_num(element)):
#     #             if (get_line_num(element) > last_line_num):
#     #                 result.append(element)
#     #                 last_line_num = get_last_line_num(element)
#     #         else: # add to result, update last_line_num
#     #             result.append(element)
#     #             last_line_num = get_last_line_num(element)

#     for element in top_level_component_id:
#         result.append(element)
#         # if (get_type(element) == "script"):
#         #     result.append(element)
#         # else:
#         #     if (get_line_num(element) > last_line_num):
#         #         result.append(element)
#         #         last_line_num = get_last_line_num(element)
#     return result

def get_first_line(code_component_id):
    '''
    get the first line if there is more than one line in a code_component
    '''
    #parse the string and find \n
    name = get_name(code_component_id)
    name_list = name.split('\n')
    return name_list[0]


lower_level_component_id = []
def get_lower_level_component(code_component_id):
    '''
    return the details of the lower-level code_componemt for each top-level code_component
    '''
    if (len(lower_level_component_id)!=0):
        i = 0
        while i < len(lower_level_component_id):
            lower_level_component_id[i] = None
            i+=1

    line_num = get_line_col_info(code_component_id, "first_char_line")
    for element in id:
        if (element != 1 and element != code_component_id):
            if (get_line_col_info(element, "first_char_line") == line_num):
                lower_level_component_id.append(element)
    return lower_level_component_id


def get_code_component_id_eval(evaluation_id):
    '''
    get the code_component_id from evaluation_id in the evaluation table
    '''
    temp = []
    c.execute('SELECT id, code_component_id from evaluation where trial_id = ?', (run_num,))
    for row in c:
        for char in row:
            temp.append(char)

    id_code_pair = {}
    a=1
    #loop through index
    for i in range(len(temp)):
        if (i % 2 != 0):
            id_code_pair[a] = temp[i]
            a = a+1
    return id_code_pair.get(evaluation_id)

def get_value_eval(evaluation_id):
    '''
    get repr(value) from evaluation table
    '''
    id_value_pair = {}
    c.execute('SELECT id, repr from evaluation where trial_id = ?', (run_num,))
    i=1
    for row in c:
        for char in row:
            if (isinstance(char, str)):
                id_value_pair[i] = char
                i = i+1
    return id_value_pair.get(evaluation_id)

def get_time_eval(evaluation_id):
    '''
    get time from evaluation table
    '''
    temp = []
    c.execute('SELECT id, checkpoint from evaluation where trial_id = ?', (run_num,))
    for row in c:
        for char in row:
            temp.append(char)

    id_time_pair = {}
    a=1
    #loop through index
    for i in range(len(temp)):
        if (i % 2 != 0):
            id_time_pair[a] = temp[i]
            a = a+1
    return id_time_pair.get(evaluation_id)

eval_id = []
def get_eval_id():
    '''
    get a list of evaluation_id in the evaluation dependency_table_id
    '''
    c.execute('SELECT id from evaluation where trial_id = ?', (run_num,))
    for row in c:
        for char in row:
            eval_id.append(char)
    return eval_id

top_eval_id = []
def get_top_level_eval_id():
    '''
    for every eval id, if value (repr) is not <class ..., add
    '''
    get_eval_id()
    for element in eval_id:
        if (get_type(get_code_component_id_eval(element)) == "name" or 
            get_type(get_code_component_id_eval(element)) == "function_def" or 
            get_type(get_code_component_id_eval(element)) == "call"):
            if (get_value_eval(element)!=str(None)):
                value = get_value_eval(element).split(" ")
                if value[0] != '<module':
                    top_eval_id.append(element)
    return top_eval_id

def value_type(value):
    '''
    check whether a value is File or not
    get the repr from the evaluation table,
    if there is a "_io.TextIOWrapper" in the string
    the type of the value will be File
    '''
    value = value.split(" ")
    for element in value:
        if (element == '<_io.TextIOWrapper' or element == '_io.TextIOWrapper'):
            return "File"
    return "Data"

def file_mode(name):

    '''
    if the file_access table exist, get file information
    '''
    #get the count of tables with the name
    c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='file_access' ''')
    #if the count is 1, then table exists
    if c.fetchone()[0]==1:

        #get hash value from table
        temp = []
        c.execute('SELECT name, mode from file_access where trial_id = ?', (run_num,))
        for row in c:
            for char in row:
                temp.append(char)

        name_mode_pair = {}

        i = 0
        while i <  len(temp)-1:
            name_mode_pair[temp[i]] = temp[i+1]
            i=i+2
        return name_mode_pair.get(name)

def file_access_table(name):

    stripped_name = name.strip("'")

    '''
    if the file_access table exist, get file information
    '''
    #get the count of tables with the name
    #c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='file_access' ''')
    #if the count is 1, then table exists
    if (file_mode(stripped_name) == "rU"):

        #get hash value from table
        temp = []
        c.execute('SELECT name, content_hash_before from file_access where trial_id = ?', (run_num,))
        for row in c:
            for char in row:
                temp.append(char)

        name_hash_pair = {}

        i = 0
        while i <  len(temp)-1:
            name_hash_pair[temp[i]] = temp[i+1]
            i=i+2
        return name_hash_pair.get(stripped_name)

def file_loc(name):
    '''
    if the file_access table exist, get file location
    '''
    hash_value = file_access_table(name)
    if (hash_value != None):

        result = input_db_file.split("/")
        #pop db file
        result.pop(-1)
        result.append("content")
        #split hash value into a list of integers
        res = [x for x in str(hash_value)] 
        #first two numbers
        first_two = res[:2]
        #add file hash value to the location
        result.append("".join(first_two))
        #remaining numbers
        remaining = res[2:]
        result.append("".join(remaining))
        return "/".join(result)

def file_loc_simple(name):
    '''
    if the file_access table exist, get file location
    '''
    hash_value = file_access_table(name)
    if (hash_value != None):

        result = input_db_file.split("/")
        #pop db file
        result.pop(-1)
        result.pop(-1)
        result.append(name)
        return "/".join(result)

def get_elapsedTime(code_component_id):
    '''
    get existing elapsedTime for procedure nodes, code-component -> code_block -> activation
    '''
    c.execute('SELECT id from code_block where trial_id = ?', (run_num,))
    code_block_id = []
    for row in c:
        for char in row:
            code_block_id.append(char)

    exist = False
    #check if this code_component has elapsedTime
    for element in code_block_id:
        if (code_component_id == element):
            exist = True

    if (exist == True):
        c.execute('SELECT start_checkpoint, code_block_id from activation where trial_id = ?', (run_num,))
        activation = []
        for row in c:
            for char in row:
                activation.append(char)

        id_time_pair = {}
        
        #loop through index
        for i in range(1, len(activation)):
            if (i % 2 != 0):
                id_time_pair[activation[i]] = activation[i-1]
        return id_time_pair.get(code_component_id)

    else:
        return -1  

def elpasedTime_timeStamp(elpasedTime):
    '''
    change elpasedTime in data nodes into timeStamp
    '''
    c.execute('SELECT start from trial where id = ?', (run_num,))
    for row in c:
        for element in row:
            tempStampParser = element.replace('.', ' ').replace(':', ' ').split() 
    elpasedTimeParser = str(elpasedTime).split(".")
    newMSecond = int(tempStampParser[-1]) + int(elpasedTimeParser[-1])
    newSecond = int(tempStampParser[-2]) + int(elpasedTimeParser[0])
    tempStampParser[-2] = str(newSecond)
    tempStampParser[-1] = str(newMSecond)
    return tempStampParser[0] + " " + tempStampParser[1] + ":" + tempStampParser[2] + ":" + tempStampParser[3] + "." + tempStampParser[4]

def check_valueType(value):

    #get container
    temp = list(value)
    valType = ""
    container = ""
    singleType = ""

    if (temp[0] == '<'):
        singleType = "str"
    else:
        try:
            valType = ast.literal_eval(value)
            if isinstance(valType, dict):
                container = "dict"
            elif isinstance(valType, set):
                container = "set"
            elif isinstance(valType, list):
                container = "list"
            elif isinstance(valType, tuple):
                container = "tuple"
            elif isinstance(valType, str):
                singleType = "str"
            elif isinstance(valType, int):
                singleType = "int"
            elif isinstance(valType, float):
                singleType = "float"
        except ValueError:
            singleType = "str"
    if (valType == None):
        singleType = ""

    #get dimension
    dimension = []
    if (container == "dict" or container == "set" or container == "list" or container == "tuple"):
        dimension.append(len(valType))

    #get type
    typeField = []
    if (container == "list" or container == "tuple"):
        for element in valType:
            exist = False
            temp = str(type(element))
            for element in typeField:
                if element == temp:
                    exist = True
            if (exist == False):
                typeField.append(temp)
    elif (container == "dict" or container == "set"):
        #check key and value type
        for element in valType:
            exist1 = False
            exist2 = False
            temp1 = str(type(element))
            temp2 = str(type(valType[element]))
            for element in typeField:
                if element == temp1:
                    exist1 = True
                if element == temp2:
                    exist2 = True
            if (exist1 == False):
                typeField.append(temp1)
            if (exist2 == False and exist2 != exist1):
                typeField.append(temp2)




    #get the final dictionary
    if (container == ""):
        return singleType
    else:
        typeDict = {}
        typeDict["container"] = container
        typeDict["dimension"] = dimension
        typeDict["type"] = typeField

        return typeDict

def isDp(code_component_id):
    if (get_type(code_component_id) == "name" and get_mode(code_component_id) == "read"):
        return True
    return False

def get_environment_info(attribute_id_num):
    c.execute('SELECT value from environment_attr where id = ?', (attribute_id_num,))
    for element in c:
        envir = element
    return envir[0]

def getOS():
    return get_environment_info(1) + get_environment_info(108)

def get_script():
    c.execute('SELECT value from argument where trial_id = ? and id = ?', (run_num,1,))
    for element in c:
        script = element
    return script[0].strip("'")

def get_script_time():
    c.execute('SELECT finish from trial where id = ?', (run_num,))
    for element in c:
        start = element
    return start[0]

def get_prov_time():
    c.execute('SELECT timestamp from tag where trial_id = ?', (run_num,))
    for element in c:
        start = element
    return start[0]

def get_total_elapsedTime():
    c.execute('SELECT start from trial where id = ?', (run_num,))
    for element in c:
        start = element
    startTime = start[0].split() 
    finishTime = get_script_time().split() 
    FMT = '%H:%M:%S.%f'
    tdelta = datetime.strptime(finishTime[1], FMT) - datetime.strptime(startTime[1], FMT)
    result = str(tdelta).replace('.', ' ').replace(':', ' ').split()
    time = ""
    if (result[0] != '0'):
        time = result[0]
        time +=":"
        time += result[1]
        time +=":"
        time += result[2]
        time +="."
        time += result[3]

    elif (result[1] != '00'):
        time += result[1]
        time +=":"
        time += result[2]
        time +="."
        time += result[3]
    elif (result[2] != '00'):
        time += result[2]
        time +="."
        time += result[3]
    elif (result[3] != '000000'):
        time += "0"
        time +="."
        time += result[3]
    return time

def getVersion(library):
    version = ""
    libraryValue = __import__(library, fromlist=[''])
    try:
        version = libraryValue.__version__
    except AttributeError:
        version = get_environment_info(139)
    return version

def sourceScript_timestamp(file_path):
    # Get file's Last modification time stamp only in terms of seconds since epoch 
    modTimesinceEpoc = os.path.getmtime(file_path) 
    # Convert seconds since epoch to readable timestamp
    modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modTimesinceEpoc))
    return modificationTime

def isDuplicate(ss_list, ss_list_element):
    '''
    check if the sourcescript list contains duplicates
    '''
    for element in ss_list:
        if (element == ss_list_element):
            return True
    return False

def prefix():
    prefix = {}
    prefix["prov"] = "http://www.w3.org/ns/prov#"
    prefix["rdt"] = "https://github.com/End-to-end-provenance/ExtendedProvJson/blob/master/JSON-format.md" 
    print(json.dumps(prefix, indent=4))
    return prefix

def agent():
    agent = {}
    a1 = {}
    a1["rdt:tool.name"] = "noworkflow"
    a1["rdt:tool.version"] = get_environment_info(140)
    a1["rdt:json.version"] = "2.3"
    agent["rdt:a1"] = a1
    print(json.dumps(agent, indent=4))
    return agent

activity = {}
def activityKey():
    #procedure nodes
    get_top_level_component_id()
    length2 = len(result)
    j = 0
    while j < length2:
        procedure_node = {}
        procedure_node["rdt:name"] = get_first_line(result[j])
        procedure_node["rdt:type"] = "Operation"
        if (get_elapsedTime(result[j]) == None):
            procedure_node["rdt:elapsedTime"] = -1
        else:
            procedure_node["rdt:elapsedTime"] = get_elapsedTime(result[j])
        procedure_node["rdt:scriptNum"] = 1
        procedure_node["rdt:startLine"] = get_line_col_info(result[j], "first_char_line")
        procedure_node["rdt:startCol"] = get_line_col_info(result[j], "first_char_column")
        procedure_node["rdt:endLine"] = get_line_col_info(result[j], "last_char_line")
        procedure_node["rdt:endCol"] = get_line_col_info(result[j], "last_char_column")

        activity["rdt:p" + str(j+1)] = procedure_node
        j = j+1
    print(json.dumps(activity, indent=4))
    return activity  

d_evalId = {}
def entityKey():

    sourcedScripts = []
    sourcedScripts_hash = []
    #data nodes
    get_top_level_eval_id()
    length = len(top_eval_id)
    number = 0
    entity = {}
    while number < length:
        data_node = {}
        data_node["rdt:name"] = get_name(get_code_component_id_eval(top_eval_id[number]))
        data_node["rdt:value"] = get_value_eval(top_eval_id[number])
        data_node["rdt:valType"] = str(check_valueType(get_value_eval(top_eval_id[number])))
        if (file_access_table(get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:type"] = "Data"
        else:
            data_node["rdt:type"] = "File"
        data_node["rdt:scope"]  = ""
        data_node["rdt:fromEnv"] = False
        if (file_access_table(get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:hash"] = ""
        else:
            data_node["rdt:hash"] = file_access_table(get_value_eval(top_eval_id[number]))  
        data_node["rdt:timestamp"] = elpasedTime_timeStamp(get_time_eval(top_eval_id[number]))
        if (file_loc(get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:location"] = ""
        else:
            data_node["rdt:location"] = file_loc(get_value_eval(top_eval_id[number]))
            if (isDuplicate(sourcedScripts, file_loc_simple(get_value_eval(top_eval_id[number]))) == False):      
                sourcedScripts.append(file_loc_simple(get_value_eval(top_eval_id[number])))
                sourcedScripts_hash.append(file_loc(get_value_eval(top_eval_id[number])))

        entity["rdt:d" + str(number+1)] = data_node
        d_evalId[top_eval_id[number]] = number+1
        number = number + 1

    #environment:
    sourceScript_ts = []
    for element in sourcedScripts_hash:
        sourceScript_ts.append(sourceScript_timestamp(element))

    environment = {}

    environment["rdt:name"] = "environment"
    environment["rdt:architecture"] = get_environment_info(136)
    environment["rdt:operatingSystem"] = getOS()
    environment["rdt:language"] = "R"
    environment["rdt:langVersion"] = "Python version " + get_environment_info(139)
    environment["rdt:script"] = get_script()
    environment["rdt:scriptTimeStamp"] = get_script_time()
    environment["rdt:totalElapsedTime"] = get_total_elapsedTime()
    if (len(sourcedScripts) == 0):
        environment["rdt:sourcedScripts"] = ""
        environment["rdt:sourcedScriptTimeStamps"] = ""
    else:   
        environment["rdt:sourcedScripts"] = sourcedScripts
        environment["rdt:sourcedScriptTimeStamps"] = sourceScript_ts
    environment["rdt:workingDirectory"] = get_environment_info(121)
    environment["rdt:provDirectory"] = get_environment_info(121) + "/.noworkflow"
    environment["rdt:provTimestamp"] = get_prov_time()
    environment["rdt:hashAlgorithm"] = "SHA 1"

    entity["rdt:environment"] = environment


    #library nodes
    #check import statements in cc table
    library_count = 1
    prov_type = {}
    prov_type["$"] = "prov:Collection"
    prov_type["type"] = "xsd:QName"

    for element in result:
        name = get_name(element).split()
        if (name[0] == "import"):
            library = {}
            library["name"] = name[1]
            library["version"] = getVersion(name[1])
            library["prov_type"] = prov_type
            entity["rdt:l" + str(library_count)] = library
            library_count += 1

    print(json.dumps(entity, indent=4))
    return entity

def pp():
    #pp edges
    wasInformedBy = {}
    t = 1
    while t < len(activity):
        ppDict = {}
        ppDict["prov:informant"] = "rdt:p" + str(t)
        ppDict["prov:informed"] = "rdt:p" + str(t+1)
        wasInformedBy["rdt: pp" + str(t)] = ppDict

        t = t+1
    print(json.dumps(wasInformedBy, indent=4))
    return wasInformedBy

def write_json(dictionary, output_json_file):
    with open(output_json_file, 'w') as outfile:
        json.dump(dictionary, outfile, indent=4)

wasGeneratedBy = {}
used = {}
def edges():
    '''
    pd edges
    get procedure nodes, get their cc_id
    for every cc_id, get its lower_level_cc_id_list
    for every data nodes, if not 1, if name is not int, get eval_id, get code_component_id
    for these cc_id, if its in lower_level_cc_id_list, get that top_level_cc_id, get_procedure nodes id
    '''
    result2 = result[1:]

    data = top_eval_id
    data2 = []
    #if name is not int or float
    for element in data:
        try:
            nameType = ast.literal_eval(get_name(get_code_component_id_eval(element)))
            if (not isinstance(nameType, int) and not isinstance(nameType, float)):
                data2.append(element)
        except ValueError:
            data2.append(element)

    count_pd = 1
    count_dp = 1
    temp = 2
    for element3 in result2:
        lower_cc_list = get_lower_level_component(element3)
        for data2_index in range(len(data2)):
            if (get_code_component_id_eval(data2[data2_index]) == element3):
                pd = {}
                pd["prov:activity"] = "rdt:p" + str(temp)
                pd["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[data2_index]))
                wasGeneratedBy["rdt:pd" + str(count_pd)] = pd
                count_pd = count_pd + 1
            else:          
                for element2 in lower_cc_list:
                    if (get_code_component_id_eval(data2[data2_index])==element2):
                        #check if it is dp edges
                        #for data nodes
                        #if code_component table shows 'x', 'name', 'r'
                        #than it is dp instead of pd
                        n = get_name(get_code_component_id_eval(data2[data2_index]))
                        v = get_value_eval(data2[data2_index])
                        prev_index = data2_index-1
                        prev_n = get_name(get_code_component_id_eval(data2[prev_index]))
                        prev_v = get_value_eval(data2[prev_index])

                        if (isDp(element2) == False):
                            #check whether there's duplicates
                            hasDuplicate = False
                            while(prev_index>0):
                                if (n == prev_n and v == prev_v):
                                    if (preTemp!=temp):
                                        pd = {}
                                        pd["prov:activity"] = "rdt:p" + str(temp)
                                        pd["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[prev_index]))
                                        wasGeneratedBy["rdt:pd" + str(count_pd)] = pd
                                        count_pd = count_pd + 1
                                    hasDuplicate = True;
                                    break;
                                prev_index-=1
                                prev_n = get_name(get_code_component_id_eval(data2[prev_index]))
                                prev_v = get_value_eval(data2[prev_index])

                            if (hasDuplicate == False):
                                pd = {}
                                pd["prov:activity"] = "rdt:p" + str(temp)
                                pd["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[data2_index]))
                                wasGeneratedBy["rdt:pd" + str(count_pd)] = pd
                                count_pd = count_pd + 1
                                preTemp = temp
                        else:
                            #if the previous data nodes has same name and value, than pass, and create a dp for the previous data node
                            while(n != prev_n or v != prev_v and prev_index>0):
                                prev_index -=1
                                prev_n = get_name(get_code_component_id_eval(data2[prev_index]))
                                prev_v = get_value_eval(data2[prev_index])
                            if (n!=prev_n or v !=prev_v):

                                dp = {}
                                dp["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[data2_index]))
                                dp["prov:activity"] = "rdt:p" + str(temp)
                                used["rdt:dp" + str(count_dp)] = dp
                                count_dp = count_dp + 1
                            else:

                                dp = {}
                                dp["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[prev_index]))
                                dp["prov:activity"] = "rdt:p" + str(temp)
                                used["rdt:dp" + str(count_dp)] = dp
                                count_dp = count_dp + 1
        temp += 1
    print(json.dumps(wasGeneratedBy, indent=4))
    print(json.dumps(used, indent=4))
    return wasGeneratedBy, used



def __main__():

    #output to a json file
    outputdict = {}
    outputdict["prefix"] = prefix()
    outputdict["agent"] = agent()
    outputdict["activity"] = activityKey()
    outputdict["entity"] = entityKey()
    outputdict["wasInformedBy"] = pp()
    edges()
    outputdict["wasGeneratedBy"] = wasGeneratedBy
    outputdict["used"] = used
    write_json(outputdict, "/Users/huiyunpeng/Desktop/J2.json")
__main__()



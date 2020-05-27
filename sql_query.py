import sqlite3
import json
import ast
import pandas
import os

#Huiyun Peng
#10 Mar 2020

#running noworkflow command: python3 -m noworkflow run test5.py

#data is stored in input_db_file
#call cursor() to create an object of it and use its execute() method to perform SQL
# input_db_file = '/Users/huiyunpeng/Desktop/demo_1/.noworkflow/db.sqlite'
# run_num = 13
#trail 13 is x=1, print(x)

input_db_file = '/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
run_num = 5

db = sqlite3.connect(input_db_file, uri=True)
c = db.cursor()

# code_component_id = []
# def get_code_component_id():
#     '''
#     returns a list of code_component_id from the evaluation table
#     '''
#     c.execute('SELECT code_component_id from evaluation where trial_id = ?', (run_num,))
#     for row in c:
#         for char in row:
#             code_component_id.append(char)
#     return code_component_id


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

def get_line_num(code_component_id):
    '''
    get first_char_line from code_component_id in code_component table
    '''
    temp = []
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

def get_col_num(code_component_id):
    '''
    get first_char_column from code_component_id in code_component table
    '''
    temp = []
    c.execute('SELECT id, first_char_column from code_component where trial_id = ?', (run_num,))
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

def get_last_line_num(code_component_id):
    '''
    get last_char_line from code_component_id in code_component table
    '''
    temp = []
    c.execute('SELECT id, last_char_line from code_component where trial_id = ?', (run_num,))
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

def get_last_col_num(code_component_id):
    '''
    get last_char_column from code_component_id in code_component table
    '''
    temp = []
    c.execute('SELECT id, last_char_column from code_component where trial_id = ?', (run_num,))
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

# def get_mode(code_component_id):
#     '''
#     get mode from code_component_id in code_component table
#     '''
#     id_mode_pair = {}
#     c.execute('SELECT id, mode from code_component where trial_id = ?', (run_num,))
#     i=1
#     for row in c:
#         for char in row:
#             if (isinstance(char, str)):
#                 if (char == "r"):
#                     id_mode_pair[i] = "read"
#                 elif (char == "w"):
#                     id_mode_pair[i] = "written"
#                 elif (char == "d"):
#                     id_mode_pair[i] = "deleted"
#                 else:
#                     id_mode_pair[i] = char
#                 i = i+1
#     return id_mode_pair.get(code_component_id)
#

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
        #skip the first line
        if (get_type(element) == "script"):
            top_level_component_id.append(element)
        else:
            if (line_num != get_line_num(element)):
                top_level_component_id.append(element)
                line_num = get_line_num(element)


    #for code component with mutiple lines
    #if it is in a single line and line_num <= last_line_num: remove
    last_line_num = 0
    for element in top_level_component_id:
        if (get_type(element) == "script" or get_type(element) == "function_def" ):
            result.append(element)
        else:
            if (get_last_line_num(element) == get_line_num(element)):
                if (get_line_num(element) > last_line_num):
                    result.append(element)
                    last_line_num = get_last_line_num(element)
            else: # add to result, update last_line_num
                result.append(element)
                last_line_num = get_last_line_num(element)


    # #omit the import statement
    # for element in result:

    #     valid = True
    #     name = get_name(element).split()
    #     for element in name:
    #         if (element == "import"):
    #             valid = False
    #     if (valid == True):
    #         final.append(element)

    return result

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
    line_num = get_line_num(code_component_id)
    for element in id:
        if (element != 1 and element != code_component_id):
            if (get_line_num(element) == line_num):
                lower_level_component_id.append(element)

    return lower_level_component_id

# def dependency_table_id_num():
#     '''
#     get the number of ids in the dependency table
#     '''
#     c.execute('SELECT id from dependency where trial_id = ?', (run_num,))
#     temp = []
#     for row in c:
#         for char in row:
#             temp.append(char)
#     return len(temp)

# def get_dependent_id(dependency_table_id):
#     '''
#     get the dependent_id from id in the dependency table, this is also the evaluation_id
#     '''
#     temp = []
#     c.execute('SELECT id, dependent_id from dependency where trial_id = ?', (run_num,))
#     for row in c:
#         for char in row:
#             temp.append(char)
#
#     id_dependent_pair = {}
#     a=1
#     #loop through index
#     for i in range(len(temp)):
#         if (i % 2 != 0):
#             id_dependent_pair[a] = temp[i]
#             a = a+1
#     return id_dependent_pair.get(dependency_table_id)

# def get_dependency_id(dependency_table_id):
#     '''
#     get the dependency_id from id in the dependency table, this is also the evaluation_id
#     '''
#     temp = []
#     c.execute('SELECT id, dependency_id from dependency where trial_id = ?', (run_num,))
#     for row in c:
#         for char in row:
#             temp.append(char)
#
#     id_dependency_pair = {}
#     a=1
#     for i in range(len(temp)):
#         if (i % 2 != 0):
#             id_dependency_pair[a] = temp[i]
#             a = a+1
#     return id_dependency_pair.get(dependency_table_id)

# def get_d_type(dependency_table_id):
#     '''
#     get type from id in the dependency table
#     '''
#     id_type_pair = {}
#     c.execute('SELECT id, type from dependency where trial_id = ?', (run_num,))
#     i=1
#     for row in c:
#         for char in row:
#             if (isinstance(char, str)):
#                 id_type_pair[i] = char
#                 i = i+1
#     return id_type_pair.get(dependency_table_id)

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

        #print('Table exists.')
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

        #print('Table exists.')
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


def write_json(dictionary, output_json_file):
    with open(output_json_file, 'w') as outfile:
        json.dump(dictionary, outfile, indent=4)



def __main__():
    get_top_level_component_id()
    length2 = len(result)
    j = 0
    activity = {}
    while j < length2:

        procedure_node = {}
        procedure_node["rdt:name"] = get_first_line(result[j])
        procedure_node["rdt:type"] = "Operation"
        procedure_node["rdt:elapsedTime"] = get_elapsedTime(result[j])
        procedure_node["rdt:scriptNum"] = 1
        procedure_node["rdt:startLine"] = get_line_num(result[j])
        procedure_node["rdt:startCol"] = get_col_num(result[j])
        procedure_node["rdt:endLine"] = get_last_line_num(result[j])
        procedure_node["rdt:endCol"] = get_last_col_num(result[j])

        activity["rdt:p" + str(j+1)] = procedure_node

        #print(procedure_node)
        #print(json.dumps(procedure_node, indent=4))

        j = j+1
        # print("id: p" + str(j+1))
        # print("name: " + get_first_line(result[j]))
        # print("startLine: " + str(get_line_num(result[j])))
        # print("startCol: " + str(get_col_num(result[j])))
        # print("endLine: " + str(get_last_line_num(result[j])))
        # print("endCol: " + str(get_last_col_num(result[j])))
        # print()
        # j = j+1
    print(json.dumps(activity, indent=4))



    get_eval_id()
    length = len(eval_id)
    number = 0
    entity = {}
    while number < length:
        data_node = {}
        data_node["rdt:name"] = get_name(get_code_component_id_eval(number+1))
        data_node["rdt:value"] = get_value_eval(number+1)
        data_node["rdt:valType"] = get_type(get_code_component_id_eval(number+1))
        data_node["rdt:type"] = value_type(get_value_eval(number+1))
        data_node["rdt:scope"]  = ""
        data_node["rdt:fromEnv"] = False
        data_node["rdt:hash"] = file_access_table(get_value_eval(number+1))
        data_node["rdt:timestamp"] = elpasedTime_timeStamp(get_time_eval(number+1))
        data_node["rdt:location"] = file_loc(get_value_eval(number+1))

        entity["rdt:d" + str(number+1)] = data_node


        # print(data_node)
        # print(json.dumps(data_node, indent=4))

        number = number + 1 
    print(json.dumps(entity, indent=4))
        # print("type: " + value_type(get_value_eval(number+1)))
        # print("id: d" + str(number+1))
        # print("name: " + get_name(get_code_component_id_eval(number+1)))
        # print("value: " + get_value_eval(number+1))
        # print("value type: "+ get_type(get_code_component_id_eval(number+1)))
        # print("scope: ")
        # print("from env: ")
        # print("time: " + str(get_time_eval(number+1)))
        # print("hash: ")
        # print("loc: ")
        # print()
        # number = number+1

    #output to a json file
    outputdict = {}
    outputdict["activity"] = activity
    outputdict["entity"] = entity

    write_json(outputdict, "/Users/huiyunpeng/Desktop/J3.json")

__main__()

import sqlite3
import json
import ast
import pandas
import os

#Huiyun Peng
#10 Mar 2020



#data is stored in input_db_file
#call cursor() to create an object of it and use its execute() method to perform SQL
# input_db_file = '/Users/huiyunpeng/Desktop/demo_1/.noworkflow/db.sqlite'
# run_num = 13
#trail 13 is x=1, print(x)

input_db_file = '/Users/huiyunpeng/Desktop/.noworkflow/db.sqlite'
run_num = 1

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


def __main__():
    get_top_level_component_id()
    length2 = len(result)
    j = 0
    while j < length2:
        print("id: p" + str(j+1))
        print("name: " + get_first_line(result[j]))
        print("startLine: " + str(get_line_num(result[j])))
        print("startCol: " + str(get_col_num(result[j])))
        print("endLine: " + str(get_last_line_num(result[j])))
        print("endCol: " + str(get_last_col_num(result[j])))
        print()
        j = j+1

    get_eval_id()
    length = len(eval_id)
    number = 0
    while number < length:
        print("type: " + value_type(get_value_eval(number+1)))
        print("id: d" + str(number+1))
        print("name: " + get_name(get_code_component_id_eval(number+1)))
        print("value: " + get_value_eval(number+1))
        print("value type: "+ get_type(get_code_component_id_eval(number+1)))
        print("scope: ")
        print("from env: ")
        print("time: " + str(get_time_eval(number+1)))
        print("hash: ")
        print("loc: ")
        print()
        number = number+1

__main__()

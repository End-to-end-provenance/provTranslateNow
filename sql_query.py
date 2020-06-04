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

#complex files
# input_db_file = '/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
# run_num = 5

#simple ones
input_db_file = '/Users/huiyunpeng/Desktop/.noworkflow/db.sqlite'
run_num = 4

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
    if (len(lower_level_component_id)!=0):
        i = 0
        while i < len(lower_level_component_id):
            lower_level_component_id[i] = None
            i+=1

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

top_eval_id = []
def get_top_level_eval_id():
    '''
    for every eval id, if value (repr) is <class ..., omit
    '''
    get_eval_id()
    index = 1
    for element in eval_id:
        value = get_value_eval(element).split(" ")
        if(value[0] != '<class'):
            top_eval_id.append(index)
        index = index + 1
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

def getPyVersion():
    c.execute('SELECT value from environment_attr where id = ?', (139,))
    for element in c:
        pyVersion = element
    return pyVersion[0]

def getOS():
    c.execute('SELECT value from environment_attr where id = ?', (1,))
    for element in c:
        os = element

    c.execute('SELECT value from environment_attr where id = ?', (108,))
    for element in c:
        osVersion = element
    return os[0] + osVersion[0]

def getWD():
    c.execute('SELECT value from environment_attr where id = ?', (121,))
    for element in c:
        wd = element
    return wd[0]

def get_arch():
    c.execute('SELECT value from environment_attr where id = ?', (136,))
    for element in c:
        ar = element
    return ar[0]

def get_script():
    c.execute('SELECT value from argument where trial_id = ? and id = ?', (run_num,1,))
    for element in c:
        script = element
    return script[0]

def get_script_time():
    c.execute('SELECT start from trial where id = ?', (run_num,))
    for element in c:
        start = element
    return start[0]


def getVersion(library):
    version = ""
    libraryValue = __import__(library, fromlist=[''])
    try:
        version = libraryValue.__version__
    except AttributeError:
        version = getPyVersion()
    return version


def write_json(dictionary, output_json_file):
    with open(output_json_file, 'w') as outfile:
        json.dump(dictionary, outfile, indent=4)

def __main__():

    #procedure nodes
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
        j = j+1
    print(json.dumps(activity, indent=4))


    #data nodes
    #get_eval_id()
    get_top_level_eval_id()
    length = len(top_eval_id)
    number = 0
    entity = {}
    d_evalId = {}
    while number < length:
        data_node = {}
        data_node["rdt:name"] = get_name(get_code_component_id_eval(top_eval_id[number]))
        data_node["rdt:value"] = get_value_eval(top_eval_id[number])
        data_node["rdt:valType"] = check_valueType(get_value_eval(top_eval_id[number]))
        #data_node["rdt:valType"] = get_type(get_code_component_id_eval(number+1))
        data_node["rdt:type"] = value_type(get_value_eval(top_eval_id[number]))
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

        entity["rdt:d" + str(number+1)] = data_node
        d_evalId[top_eval_id[number]] = number+1
        number = number + 1

    #environment:
    environment = {}

    environment["rdt:name"] = "environment"
    environment["rdt:architecture"] = get_arch()
    environment["rdt:operatingSystem"] = getOS()
    environment["rdt:language"] = "Python"
    environment["rdt:langVersion"] = "Python version " + getPyVersion()
    environment["rdt:script"] = get_script()
    environment["rdt:scriptTimeStamp"] = get_script_time()
    environment["rdt:totalElapsedTime"] = ""
    environment["rdt:sourcedScripts"] = ""
    environment["rdt:sourcedScriptTimeStamps"] = ""
    environment["rdt:workingDirectory"] = getWD()
    environment["rdt:provDirectory"] = getWD() + "/.noworkflow"
    environment["rdt:provTimestamp"] = ""
    environment["rdt:hashAlgorithm"] = ""

    entity["rdt:environment"] = environment


    #library nodes
    #check import statements in cc table
    library_count = 1
    prov_type = {}
    prov_type["$"] = "prov:Collection"
    prov_type["type"] = "xsd:QName"

    for element in result:
        name = get_name(element).split()
        for element in name:
            if (element == "import"):
                library = {}
                library["name"] = name[1]
                library["version"] = getVersion(name[1])
                library["prov_type"] = prov_type
                entity["rdt:l" + str(library_count)] = library
                library_count += 1

    print(json.dumps(entity, indent=4))


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

    '''
    pd edges
    get procedure nodes, omit 1, get their cc_id
    for every cc_id, get its lower_level_cc_id_list
    for every data nodes, if not 1, if name is not int, get eval_id, get code_component_id
    for these cc_id, if its in lower_level_cc_id_list, get that top_level_cc_id, get_procedure nodes id
    '''
    result2 = result[1:]

    data = top_eval_id[1:]
    data2 = []
    #if name is not int or float
    for element in data:
        try:
            nameType = ast.literal_eval(get_name(get_code_component_id_eval(element)))
            if (not isinstance(nameType, int) and not isinstance(nameType, float)):
                data2.append(element)
        except ValueError:
            data2.append(element)

    wasGeneratedBy = {}
    used = {}
    count_pd = 1
    count_dp = 1
    temp = 2
    for element3 in result2:
        lower_cc_list = get_lower_level_component(element3)
        for element in data2:
            for element2 in lower_cc_list:
                if (get_code_component_id_eval(element)==element2):
                    #check if it is dp edges
                    #for data nodes
                    #if code_component table shows 'x', 'name', 'r'
                    #than it is dp instead of pd
                    if (isDp(element2) == False):
                        pd = {}
                        pd["prov:activity"] = "rdt:p" + str(temp)
                        pd["prov:entity"] = "rdt:d" + str(d_evalId.get(element))
                        wasGeneratedBy["rdt:pd" + str(count_pd)] = pd
                        count_pd = count_pd + 1
                    else:
                        dp = {}
                        dp["prov:entity"] = "rdt:d" + str(d_evalId.get(element))
                        dp["prov:activity"] = "rdt:p" + str(temp)
                        used["rdt:dp" + str(count_dp)] = dp
                        count_dp = count_dp + 1

        temp += 1

    print(json.dumps(wasGeneratedBy, indent=4))
    print(json.dumps(used, indent=4))

          

    #output to a json file
    outputdict = {}
    outputdict["activity"] = activity
    outputdict["entity"] = entity
    outputdict["wasInformedBy"] = wasInformedBy
    outputdict["wasGeneratedBy"] = wasGeneratedBy
    outputdict["used"] = used

    write_json(outputdict, "/Users/huiyunpeng/Desktop/J2.json")
__main__()

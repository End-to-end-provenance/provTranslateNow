import sqlite3
import json
import ast
import os
import time
from sql_info import Sql_info
#Huiyun Peng
#10 Mar 2020

#running noworkflow command: python3 -m noworkflow run test5.py

#data is stored in input_db_file
#call cursor() to create an object of it and use its execute() method to perform SQL


#real python scripts
# input_db_file = '/Users/huiyunpeng/Desktop/demo_1/.noworkflow/db.sqlite'
# run_num = 4

input_db_file = '/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
run_num = 6

#simple ones
# input_db_file = '/Users/huiyunpeng/Desktop/.noworkflow/db.sqlite'
# run_num = 21

# input_db_file = '/Users/huiyunpeng/Desktop/.noworkflow/db.sqlite'
# run_num = 25

sql_info = Sql_info(input_db_file, run_num)

result = []
def get_top_level_component_id():
    '''
    returns a list of top-level code_component
    '''
    id = sql_info.get_code_component_id()
    top_level_component_id = []
    #for code component with single lines
    line_num = 0
    for element in id:
        if (sql_info.get_basic_info(element, "type") != "module"):
            #skip the first line
            if (sql_info.get_basic_info(element, "type") == "script"):
                top_level_component_id.append(element)
            else:
                if (line_num != sql_info.get_line_col_info(element, "first_char_line")):
                    top_level_component_id.append(element)
                    line_num = sql_info.get_line_col_info(element, "first_char_line")


    #for code component with mutiple lines
    #if it is in a single line and line_num <= last_line_num: remove
    last_line_num = 0

    for element in top_level_component_id:
        if (sql_info.get_basic_info(element, "type") == "script"):
            result.append(element)
        else:
            if (sql_info.get_line_col_info(element, "first_char_line") > last_line_num):
                result.append(element)
                last_line_num = sql_info.get_line_col_info(element, "last_char_line")
 
             
    return result

def get_first_line(code_component_id):
    '''
    get the first line if there is more than one line in a code_component
    '''
    #parse the string and find \n
    name = sql_info.get_basic_info(code_component_id, "name")
    name_list = name.split('\n')
    return name_list[0]
    #return name

for_loop_range = {}
def get_for_loop_range():

    for element in result:
        if (sql_info.get_basic_info(element, "type") == "for" or sql_info.get_basic_info(element, "type") == "while" or sql_info.get_basic_info(element, "type") == "if"):
            #parse for val in x:\n\tif(val%2 == 0):\n\t\tcount = count + 1
            #to val
            temp = get_first_line(element).split(" ")
            line_list = []
            line_list.append(sql_info.get_line_col_info(element, "first_char_line"))
            line_list.append(sql_info.get_line_col_info(element, "last_char_line"))
            for_loop_range[temp[1]] = line_list
    return for_loop_range

function_range = {}
def get_function_range():
    for element in result:
        if (sql_info.get_basic_info(element, "type") == "function_def"):
            temp = sql_info.get_basic_info(element, "name")
            line_list = []
            line_list.append(sql_info.get_line_col_info(element, "first_char_line"))
            line_list.append(sql_info.get_line_col_info(element, "last_char_line"))
            function_range[temp] = line_list
    return function_range

def in_for_loop(eval_cc_id):
    '''
    delete datas inside for loop iterations
    E.G. for val in x
    delete val
    '''
    for element in for_loop_range:
        #element == val
        #for_loop_range.get(element) == [3, 5]
        if (element == sql_info.get_basic_info(eval_cc_id, "name")):
            if (for_loop_range.get(element)[0] <= sql_info.get_line_col_info(eval_cc_id, "first_char_line") and sql_info.get_line_col_info(eval_cc_id, "first_char_line") <= for_loop_range.get(element)[1]):
                return True
    return False

def need_update_loop_val(eval_cc_id):
    '''
    only select the latest element in for loop iterations
    '''
    for element in for_loop_range:
        if (for_loop_range.get(element)[0] <= sql_info.get_line_col_info(eval_cc_id, "first_char_line") and sql_info.get_line_col_info(eval_cc_id, "first_char_line") <= for_loop_range.get(element)[1]):
            return True
    return False

def get_func_name(evaluation_id):
    func_name = sql_info.get_basic_info(sql_info.get_code_component_id_eval(evaluation_id), "name")
    split_func_name = func_name.split("(") 
    return split_func_name[0]

def in_function(eval_cc_id):

    for element in function_range:
        if (function_range.get(element)[0] <= sql_info.get_line_col_info(eval_cc_id, "first_char_line") and sql_info.get_line_col_info(eval_cc_id, "first_char_line") <= function_range.get(element)[1]):
                return True
    return False




top_eval_id = []
def get_top_level_eval_id():
    '''
    select top level evaluation ids
    '''
    temp_top_eval_id = []
    eval_id = sql_info.get_eval_id()
    get_function_range()
    get_for_loop_range()
    for element in eval_id:
        if (sql_info.get_basic_info(sql_info.get_code_component_id_eval(element), "type") == "name"):
            if (in_function(sql_info.get_code_component_id_eval(element)) == False):
                if (sql_info.get_value_eval(element)!=str(None) and in_for_loop(sql_info.get_code_component_id_eval(element)) == False):
                    value = sql_info.get_value_eval(element).split(" ")
                    if value[0] != '<module':
                        temp_top_eval_id.append(element)
        elif (sql_info.get_basic_info(sql_info.get_code_component_id_eval(element), "type") == "call"):
            #get all function_def names
            for el in function_range:
                if (get_func_name(element) == el):
                    temp_top_eval_id.append(element)
        elif(sql_info.get_basic_info(sql_info.get_code_component_id_eval(element), "type") == "function_def"):
            temp_top_eval_id.append(element)




    #check for/while loop
    i = 0
    while(i < len(temp_top_eval_id) - 1): 
        if (need_update_loop_val(sql_info.get_code_component_id_eval(temp_top_eval_id[i])) == True):
            if (sql_info.get_basic_info(sql_info.get_code_component_id_eval(temp_top_eval_id[i]), "mode") == "r"):
                top_eval_id.append(temp_top_eval_id[i])
            j = i + 1
            name_prev = sql_info.get_basic_info(sql_info.get_code_component_id_eval(temp_top_eval_id[i]), "name")
            temp = j
            update = False
            while(j < len(temp_top_eval_id) and need_update_loop_val(sql_info.get_code_component_id_eval(temp_top_eval_id[j])) == True):
                name_after = sql_info.get_basic_info(sql_info.get_code_component_id_eval(temp_top_eval_id[j]), "name")
                if (name_prev == name_after):
                    temp = j
                    update = True
                j = j + 1
            if (update == True):
                if (sql_info.get_basic_info(sql_info.get_code_component_id_eval(temp_top_eval_id[temp]), "mode") == "r"):
                    i = temp
                    top_eval_id.append(temp_top_eval_id[i-1])
                else:
                    i = temp
                    top_eval_id.append(temp_top_eval_id[i])
        else:
            top_eval_id.append(temp_top_eval_id[i])
        i = i + 1
    if (i < len(temp_top_eval_id)):
        top_eval_id.append(temp_top_eval_id[i])
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

def file_loc(name):
    '''
    if the file_access table exist, get file location
    '''
    hash_value = sql_info.file_access_table(name)
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
    hash_value = sql_info.file_access_table(name)
    if (hash_value != None):

        result = input_db_file.split("/")
        #pop db file
        result.pop(-1)
        result.pop(-1)
        result.append(name)
        return "/".join(result) 

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
    if (sql_info.get_basic_info(code_component_id, "type") == "name" and sql_info.get_basic_info(code_component_id, "mode") == "r"):
        return True
    return False

def getVersion(library):
    version = ""
    libraryValue = __import__(library, fromlist=[''])
    try:
        version = libraryValue.__version__
    except AttributeError:
        version = sql_info.get_environment_info(139)
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
    a1["rdt:tool.version"] = sql_info.get_environment_info(140)
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
        if (sql_info.get_elapsedTime(result[j]) == None):
            procedure_node["rdt:elapsedTime"] = -1
        else:
            procedure_node["rdt:elapsedTime"] = sql_info.get_elapsedTime(result[j])
        procedure_node["rdt:scriptNum"] = 1
        procedure_node["rdt:startLine"] = sql_info.get_line_col_info(result[j], "first_char_line")
        procedure_node["rdt:startCol"] = sql_info.get_line_col_info(result[j], "first_char_column")
        procedure_node["rdt:endLine"] = sql_info.get_line_col_info(result[j], "last_char_line")
        procedure_node["rdt:endCol"] = sql_info.get_line_col_info(result[j], "last_char_column")

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
        data_node["rdt:name"] = sql_info.get_basic_info(sql_info.get_code_component_id_eval(top_eval_id[number]), "name")
        data_node["rdt:value"] = sql_info.get_value_eval(top_eval_id[number])
        data_node["rdt:valType"] = str(check_valueType(sql_info.get_value_eval(top_eval_id[number])))
        if (sql_info.file_access_table(sql_info.get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:type"] = "Data"
        else:
            data_node["rdt:type"] = "File"
        data_node["rdt:scope"]  = ""
        data_node["rdt:fromEnv"] = False
        if (sql_info.file_access_table(sql_info.get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:hash"] = ""
        else:
            data_node["rdt:hash"] = sql_info.file_access_table(sql_info.get_value_eval(top_eval_id[number]))  
        data_node["rdt:timestamp"] = sql_info.elpasedTime_timeStamp(sql_info.get_basic_info(top_eval_id[number],"checkpoint"))
        if (file_loc(sql_info.get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:location"] = ""
        else:
            data_node["rdt:location"] = file_loc(sql_info.get_value_eval(top_eval_id[number]))
            if (isDuplicate(sourcedScripts, file_loc_simple(sql_info.get_value_eval(top_eval_id[number]))) == False):      
                sourcedScripts.append(file_loc_simple(sql_info.get_value_eval(top_eval_id[number])))
                sourcedScripts_hash.append(file_loc(sql_info.get_value_eval(top_eval_id[number])))

        entity["rdt:d" + str(number+1)] = data_node
        d_evalId[top_eval_id[number]] = number+1
        number = number + 1

    #environment:
    sourceScript_ts = []
    for element in sourcedScripts_hash:
        sourceScript_ts.append(sourceScript_timestamp(element))

    environment = {}

    environment["rdt:name"] = "environment"
    environment["rdt:architecture"] = sql_info.get_environment_info(136)
    environment["rdt:operatingSystem"] = sql_info.getOS()
    environment["rdt:language"] = "R"
    environment["rdt:langVersion"] = "Python version " + sql_info.get_environment_info(139)
    environment["rdt:script"] = sql_info.get_script()
    environment["rdt:scriptTimeStamp"] = sql_info.get_script_time()
    environment["rdt:totalElapsedTime"] = sql_info.get_total_elapsedTime()
    if (len(sourcedScripts) == 0):
        environment["rdt:sourcedScripts"] = ""
        environment["rdt:sourcedScriptTimeStamps"] = ""
    else:   
        environment["rdt:sourcedScripts"] = sourcedScripts
        environment["rdt:sourcedScriptTimeStamps"] = sourceScript_ts
    environment["rdt:workingDirectory"] = sql_info.get_environment_info(121)
    environment["rdt:provDirectory"] = sql_info.get_environment_info(121) + "/.noworkflow"
    environment["rdt:provTimestamp"] = sql_info.get_prov_time()
    environment["rdt:hashAlgorithm"] = "SHA 1"

    entity["rdt:environment"] = environment


    #library nodes
    #check import statements in cc table
    library_count = 1
    prov_type = {}
    prov_type["$"] = "prov:Collection"
    prov_type["type"] = "xsd:QName"

    for element in result:
        name = sql_info.get_basic_info(element, "name").split()
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

def inRange(eval_cc, cc):
    #start line of cc <= start line of eval_cc <= finish line of cc 
    if ((sql_info.get_line_col_info(cc, "first_char_line") <= sql_info.get_line_col_info(eval_cc, "first_char_line")) and (sql_info.get_line_col_info(eval_cc, "first_char_line") <= sql_info.get_line_col_info(cc, "last_char_line"))):
        return True
    else:
        return False

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

    data2 = top_eval_id

    count_pd = 1
    count_dp = 1
    temp = 2
    #element is cc id
    for element3 in result2:
        #lower_cc_list = get_lower_level_component(element3)
        for data2_index in range(len(data2)):
            #no lower_level_cc_id_list anymore
            #check whether the cc in eval is in the line range of a procedure node
            #if it is, create edges
            if (inRange(sql_info.get_code_component_id_eval(data2[data2_index]), element3)):
                #check if it is dp edges
                #for data nodes
                #if code_component table shows 'x', 'name', 'r'
                #than it is dp instead of pd
                n = sql_info.get_basic_info(sql_info.get_code_component_id_eval(data2[data2_index]), "name")
                v = sql_info.get_value_eval(data2[data2_index])
                prev_index = data2_index-1
                prev_n = sql_info.get_basic_info(sql_info.get_code_component_id_eval(data2[prev_index]), "name")
                prev_v = sql_info.get_value_eval(data2[prev_index])

                #check function data nodes       
                if (sql_info.get_basic_info(sql_info.get_code_component_id_eval(data2[data2_index]), "type") == "call"):
                    #dp nodes
                    func_name = get_func_name(data2[data2_index])
                    while(prev_index >= 0):
                        if (func_name == prev_n):
                            dp = {}
                            dp["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[prev_index]))
                            dp["prov:activity"] = "rdt:p" + str(temp)
                            used["rdt:dp" + str(count_dp)] = dp
                            count_dp = count_dp + 1
                            break;
                        prev_index -= 1
                        prev_n = sql_info.get_basic_info(sql_info.get_code_component_id_eval(data2[prev_index]), "name")
                else:

                    if (isDp(sql_info.get_code_component_id_eval(data2[data2_index])) == False):
                        #check whether there's duplicates
                        hasDuplicate = False
                        while(prev_index>=0):
                            if (n == prev_n and v == prev_v):
                                #check whether the line numbers of p ndoes are the same
                                if (sql_info.get_line_col_info(sql_info.get_code_component_id_eval(data2[data2_index]), "first_char_line") == 
                                    sql_info.get_line_col_info(sql_info.get_code_component_id_eval(data2[prev_index]), "first_char_line")):
                                    if (preTemp!=temp):
                                        pd = {}
                                        pd["prov:activity"] = "rdt:p" + str(temp)
                                        pd["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[prev_index]))
                                        wasGeneratedBy["rdt:pd" + str(count_pd)] = pd
                                        count_pd = count_pd + 1
                                    hasDuplicate = True;
                                    break;
                            prev_index-=1
                            prev_n = sql_info.get_basic_info(sql_info.get_code_component_id_eval(data2[prev_index]), "name")
                            prev_v = sql_info.get_value_eval(data2[prev_index])

                        if (hasDuplicate == False):
                            pd = {}
                            pd["prov:activity"] = "rdt:p" + str(temp)
                            pd["prov:entity"] = "rdt:d" + str(d_evalId.get(data2[data2_index]))
                            wasGeneratedBy["rdt:pd" + str(count_pd)] = pd
                            count_pd = count_pd + 1
                            preTemp = temp
                    else:
                        #if the previous data nodes has same name and value, than pass, and create a dp for the previous data node
                        while(n != prev_n or v != prev_v and prev_index>=0):
                            prev_index -=1
                            prev_n = sql_info.get_basic_info(sql_info.get_code_component_id_eval(data2[prev_index]), "name")
                            prev_v = sql_info.get_value_eval(data2[prev_index])
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
    write_json(outputdict, sql_info.get_environment_info(121) + "/J2.json")

__main__()



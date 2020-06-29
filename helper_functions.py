from sql_info import Sql_info
import ast
import os
import time
input_db_file = '/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
run_num = 6
sql_info = Sql_info(input_db_file, run_num)
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


def inRange(eval_cc, cc):
    #start line of cc <= start line of eval_cc <= finish line of cc 
    if ((sql_info.get_line_col_info(cc, "first_char_line") <= sql_info.get_line_col_info(eval_cc, "first_char_line")) and (sql_info.get_line_col_info(eval_cc, "first_char_line") <= sql_info.get_line_col_info(cc, "last_char_line"))):
        return True
    else:
        return False
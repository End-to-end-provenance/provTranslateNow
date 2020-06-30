from sql_info import Sql_info
import ast

class Helper_functions:

    global sql_info

    def __init__(self, sql_info):
        Helper_functions.sql_info = sql_info

    def file_loc_simple(self,name):
        '''
        if the file_access table exist, get file location
        '''
        hash_value = Helper_functions.sql_info.file_access_table(name)
        if (hash_value != None):
            result = Helper_functions.sql_info.get_environment_info(121)
            return result + "/" + name.strip("'")

    def check_valueType(self, value):

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

    def isDp(self, code_component_id):
        if (Helper_functions.sql_info.get_basic_info(code_component_id, "type") == "name" and Helper_functions.sql_info.get_basic_info(code_component_id, "mode") == "r"):
            return True
        return False

    def getVersion(self, library):
        version = ""
        libraryValue = __import__(library, fromlist=[''])
        try:
            version = libraryValue.__version__
        except AttributeError:
            version = Helper_functions.sql_info.get_environment_info(139)
        return version

    def inRange(self, eval_cc, cc):
        #start line of cc <= start line of eval_cc <= finish line of cc 
        if ((Helper_functions.sql_info.get_line_col_info(cc, "first_char_line") <= Helper_functions.sql_info.get_line_col_info(eval_cc, "first_char_line")) and (Helper_functions.sql_info.get_line_col_info(eval_cc, "first_char_line") <= Helper_functions.sql_info.get_line_col_info(cc, "last_char_line"))):
            return True
        else:
            return False
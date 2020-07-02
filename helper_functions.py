# Copyright (C) President and Fellows of Harvard College and 
# Trustees of Mount Holyoke College, 2020.

# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public
#   License along with this program.  If not, see
#   <http://www.gnu.org/licenses/>.
#
#Huiyun Peng
#10 Mar 2020
#This is the class of helper functions. 
#It contains functions to get information from sql table and convert in to the type we want
from sql_info import Sql_info
import ast

class Helper_functions:

    global sql_info

    def __init__(self, sql_info):
        Helper_functions.sql_info = sql_info

    def file_loc_simple(self, name):
        '''
        if the file_access table exist, get the file location
        parameter: name is the file name
        '''
        hash_value = Helper_functions.sql_info.file_access_table(name)
        if (hash_value != None):
            result = Helper_functions.sql_info.get_environment_info(121)
            return result + "/" + name.strip("'")

    def check_valueType(self, value):
        '''
        get the type of a value
        parameter: value is the value in the evaluation table
        '''

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
        '''
        check whether this procedure node sets a data or uses a data
        parameter: code component id in the code_component table
        '''
        if (Helper_functions.sql_info.get_basic_info(code_component_id, "type") == "name" and Helper_functions.sql_info.get_basic_info(code_component_id, "mode") == "r"):
            return True
        return False

    def getVersion(self, library):
        '''
        get the version of a python library that is imported in the script
        '''
        version = ""
        libraryValue = __import__(library, fromlist=[''])
        try:
            version = libraryValue.__version__
        except AttributeError:
            version = Helper_functions.sql_info.get_environment_info(139)
        return version

    def inRange(self, eval_cc, cc):
        '''
        check whether this data node is in the line range of this procedure node
        parameter: eval_cc: code_component_id in the evaluation table
                   cc: code_component id
        '''
        #start line of cc <= start line of eval_cc <= finish line of cc 
        if ((Helper_functions.sql_info.get_line_col_info(cc, "first_char_line") <= Helper_functions.sql_info.get_line_col_info(eval_cc, "first_char_line")) and (Helper_functions.sql_info.get_line_col_info(eval_cc, "first_char_line") <= Helper_functions.sql_info.get_line_col_info(cc, "last_char_line"))):
            return True
        else:
            return False
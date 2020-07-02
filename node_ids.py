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
#This is the class of node ids. 
#It contains functions to get code_component_id and evaluation_id from the sql table, and select the top level ids from them.
from sql_info import Sql_info

class Node_ids:

    global sql_info

    def __init__(self, sql_info):
        Node_ids.sql_info = sql_info

    result = []
    def get_top_level_component_id(self):
        '''
        returns a list of top-level code_component ids
        It only contains the first line of functions/for loops/while loops/if-else statements, and other code with only a single line
        '''
        id = Node_ids.sql_info.get_code_component_id()
        top_level_component_id = []

        #for code component with single lines
        line_num = 0
        for element in id:
            if (Node_ids.sql_info.get_basic_info(element, "type") != "module"):
                #skip the first line
                if (Node_ids.sql_info.get_basic_info(element, "type") == "script"):
                    top_level_component_id.append(element)
                else:
                    if (line_num != Node_ids.sql_info.get_line_col_info(element, "first_char_line")):
                        top_level_component_id.append(element)
                        line_num = Node_ids.sql_info.get_line_col_info(element, "first_char_line")


        #for code component with mutiple lines
        #if it is in a single line and line_num <= last_line_num: remove
        last_line_num = 0

        for element in top_level_component_id:
            if (Node_ids.sql_info.get_basic_info(element, "type") == "script"):
                Node_ids.result.append(element)
            else:
                if (Node_ids.sql_info.get_line_col_info(element, "first_char_line") > last_line_num):
                    Node_ids.result.append(element)
                    last_line_num = Node_ids.sql_info.get_line_col_info(element, "last_char_line")
     
                 
        return Node_ids.result

    def get_first_line(self, code_component_id):
        '''
        get the first few lines of code of functions, loops, and if statements
        '''

        name = Node_ids.sql_info.get_basic_info(code_component_id, "name")
        #parse the string
        name_list = name.split('\n')
        #if there are more than three lines, only shows the first three lines
        if (len(name_list) > 3):
            return name_list[0] + name_list[1] + name_list[2]
        else:
            return name

    for_loop_range = {}
    def get_for_loop_range(self):
        '''
        get the range of loops and if statements
        '''
        for element in Node_ids.result:
            if (Node_ids.sql_info.get_basic_info(element, "type") == "for" or Node_ids.sql_info.get_basic_info(element, "type") == "while" or Node_ids.sql_info.get_basic_info(element, "type") == "if"):
                #e.g. from "for val in x" get "val", and set it to the key of the dictionary, value is the range
                temp = Node_ids.get_first_line(self, element).split(" ")
                line_list = []
                line_list.append(Node_ids.sql_info.get_line_col_info(element, "first_char_line"))
                line_list.append(Node_ids.sql_info.get_line_col_info(element, "last_char_line"))
                Node_ids.for_loop_range[temp[1]] = line_list
        return Node_ids.for_loop_range

    function_range = {}
    def get_function_range(self):
        '''
        get the range of functions declarations 
        '''
        for element in Node_ids.result:
            if (Node_ids.sql_info.get_basic_info(element, "type") == "function_def"):
                temp = Node_ids.sql_info.get_basic_info(element, "name")
                #key is the function name, value is the range
                line_list = []
                line_list.append(Node_ids.sql_info.get_line_col_info(element, "first_char_line"))
                line_list.append(Node_ids.sql_info.get_line_col_info(element, "last_char_line"))
                Node_ids.function_range[temp] = line_list
        return Node_ids.function_range

    def in_for_loop(self,eval_cc_id):
        '''
        check whether the data is inside the for loop iterations
        '''
        for element in Node_ids.for_loop_range:
            if (element == Node_ids.sql_info.get_basic_info(eval_cc_id, "name")):
                if (Node_ids.for_loop_range.get(element)[0] <= Node_ids.sql_info.get_line_col_info(eval_cc_id, "first_char_line") and Node_ids.sql_info.get_line_col_info(eval_cc_id, "first_char_line") <= Node_ids.for_loop_range.get(element)[1]):
                    return True
        return False

    def need_update_loop_val(self,eval_cc_id):
        '''
        only select the last data element in for loop iterations
        '''
        for element in Node_ids.for_loop_range:
            if (Node_ids.for_loop_range.get(element)[0] <= Node_ids.sql_info.get_line_col_info(eval_cc_id, "first_char_line") and Node_ids.sql_info.get_line_col_info(eval_cc_id, "first_char_line") <= Node_ids.for_loop_range.get(element)[1]):
                return True
        return False

    def get_func_name(self,evaluation_id):
        '''
        get the function name in calls
        '''
        func_name = Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(evaluation_id), "name")
        split_func_name = func_name.split("(") 
        return split_func_name[0]

    def in_function(self,eval_cc_id):
        '''
        check whether the data is inside the functions
        '''
        for element in Node_ids.function_range:
            if (Node_ids.function_range.get(element)[0] <= Node_ids.sql_info.get_line_col_info(eval_cc_id, "first_char_line") and Node_ids.sql_info.get_line_col_info(eval_cc_id, "first_char_line") <= Node_ids.function_range.get(element)[1]):
                    return True
        return False


    def get_top_level_eval_id(self):
        '''
        returns a list of top-level code_component ids
        It only contains the value with type "name"/"call"/"function_def"
        It removes redundent data nodes inside the loop/function
        '''
        temp_top_eval_id = []
        eval_id = Node_ids.sql_info.get_eval_id()
        Node_ids.get_function_range(self)
        Node_ids.get_for_loop_range(self)

        #select the value with type "name"/"call"/"function_def"
        for element in eval_id:
            if (Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(element), "type") == "name"):
                if (Node_ids.in_function(self, Node_ids.sql_info.get_code_component_id_eval(element)) == False):
                    if (Node_ids.sql_info.get_value_eval(element)!=str(None) and Node_ids.in_for_loop(self, Node_ids.sql_info.get_code_component_id_eval(element)) == False):
                        value = Node_ids.sql_info.get_value_eval(element).split(" ")
                        if value[0] != '<module':
                            temp_top_eval_id.append(element)
            elif (Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(element), "type") == "call"):
                #get all function_def names
                for el in Node_ids.function_range:
                    if (Node_ids.get_func_name(self,element) == el):
                        temp_top_eval_id.append(element)
            elif(Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(element), "type") == "function_def"):
                temp_top_eval_id.append(element)

        top_eval_id = []
        #check whether the data is in loops and delete datas in the iterations
        i = 0
        while(i < len(temp_top_eval_id) - 1): 
            if (Node_ids.need_update_loop_val(self, Node_ids.sql_info.get_code_component_id_eval(temp_top_eval_id[i])) == True):
                if (Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(temp_top_eval_id[i]), "mode") == "r"):
                    top_eval_id.append(temp_top_eval_id[i])
                j = i + 1
                name_prev = Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(temp_top_eval_id[i]), "name")
                temp = j
                update = False
                while(j < len(temp_top_eval_id) and Node_ids.need_update_loop_val(self, Node_ids.sql_info.get_code_component_id_eval(temp_top_eval_id[j])) == True):
                    name_after = Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(temp_top_eval_id[j]), "name")
                    if (name_prev == name_after):
                        temp = j
                        update = True
                    j = j + 1
                if (update == True):
                    if (Node_ids.sql_info.get_basic_info(Node_ids.sql_info.get_code_component_id_eval(temp_top_eval_id[temp]), "mode") == "r"):
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

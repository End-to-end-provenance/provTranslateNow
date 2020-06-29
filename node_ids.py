from sql_info import Sql_info
input_db_file = '/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
run_num = 6
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

    top_eval_id = []
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

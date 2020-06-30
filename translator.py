import sqlite3
import json
from sql_info import Sql_info
from node_ids import Node_ids
from helper_functions import Helper_functions
#Huiyun Peng
#10 Mar 2020

#running noworkflow command: python3 -m noworkflow run test5.py


#set up
input_db_file = input("Enter the path of your db.sqlite file: ")
#'/Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite'
#run_num = 6
run_num = input("Enter the trial id: ")
sql_info = Sql_info(input_db_file, run_num)
node_ids = Node_ids(sql_info)
helper_functions = Helper_functions(sql_info)
result = node_ids.get_top_level_component_id()
top_eval_id = node_ids.get_top_level_eval_id()


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

    length2 = len(result)
    j = 0
    while j < length2:
        procedure_node = {}
        procedure_node["rdt:name"] = node_ids.get_first_line(result[j])
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

    #data nodes
    length = len(top_eval_id)
    number = 0
    entity = {}
    while number < length:
        data_node = {}
        data_node["rdt:name"] = sql_info.get_basic_info(sql_info.get_code_component_id_eval(top_eval_id[number]), "name")
        data_node["rdt:value"] = sql_info.get_value_eval(top_eval_id[number]).strip("'")
        data_node["rdt:valType"] = str(helper_functions.check_valueType(sql_info.get_value_eval(top_eval_id[number])))
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
        if (helper_functions.file_loc_simple(sql_info.get_value_eval(top_eval_id[number])) == None):
            data_node["rdt:location"] = ""
        else:
            data_node["rdt:location"] = helper_functions.file_loc_simple(sql_info.get_value_eval(top_eval_id[number]))

        entity["rdt:d" + str(number+1)] = data_node
        d_evalId[top_eval_id[number]] = number+1
        number = number + 1

    environment = {}

    environment["rdt:name"] = "environment"
    environment["rdt:architecture"] = sql_info.get_environment_info(136)
    environment["rdt:operatingSystem"] = sql_info.getOS()
    environment["rdt:language"] = "R"
    environment["rdt:langVersion"] = "Python version " + sql_info.get_environment_info(139)
    environment["rdt:script"] = sql_info.get_script()
    environment["rdt:scriptTimeStamp"] = sql_info.get_script_time()
    environment["rdt:totalElapsedTime"] = sql_info.get_total_elapsedTime()
    environment["rdt:sourcedScripts"] = ""
    environment["rdt:sourcedScriptTimeStamps"] = ""
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
            library["version"] = helper_functions.getVersion(name[1])
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

def edges():
    '''
    pd edges
    get procedure nodes, get their cc_id
    for every cc_id, get its lower_level_cc_id_list
    for every data nodes, if not 1, if name is not int, get eval_id, get code_component_id
    for these cc_id, if its in lower_level_cc_id_list, get that top_level_cc_id, get_procedure nodes id
    '''
    wasGeneratedBy = {}
    used = {}

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
            if (helper_functions.inRange(sql_info.get_code_component_id_eval(data2[data2_index]), element3)):
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
                    func_name = node_ids.get_func_name(data2[data2_index])
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

                    if (helper_functions.isDp(sql_info.get_code_component_id_eval(data2[data2_index])) == False):
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


def write_json(dictionary, output_json_file):
    with open(output_json_file, 'w') as outfile:
        json.dump(dictionary, outfile, indent=4)

def __main__():
    #output to a json file
    outputdict = {}
    outputdict["prefix"] = prefix()
    outputdict["agent"] = agent()
    outputdict["activity"] = activityKey()
    outputdict["entity"] = entityKey()
    outputdict["wasInformedBy"] = pp()
    wasGeneratedBy, used = edges()
    outputdict["wasGeneratedBy"] = wasGeneratedBy
    outputdict["used"] = used
    write_json(outputdict, sql_info.get_environment_info(121) + "/now.json")

__main__()



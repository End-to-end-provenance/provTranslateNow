import sqlite3
from datetime import datetime
class Sql_info:


    run_num = 0
    global c 

    def __init__(self, input_db_file, run_num):
        db = sqlite3.connect(input_db_file, uri=True)
        Sql_info.c = db.cursor()
        Sql_info.run_num = run_num


    def get_code_component_id(self):
        id = []
        Sql_info.c.execute('SELECT id from code_component where trial_id = ?', (Sql_info.run_num,))
        #get all id
        for row in Sql_info.c:
            for char in row:
                id.append(char)
        return id


    def get_eval_id(self):
        '''
        get a list of evaluation_id in the evaluation dependency_table_id
        '''
        eval_id = []
        Sql_info.c.execute('SELECT id from evaluation where trial_id = ?', (Sql_info.run_num,))
        for row in Sql_info.c:
            for char in row:
                eval_id.append(char)
        return eval_id


    def get_basic_info(self, id, info):
        #cc_id
        #get name from code_component_id in code_component table
        if (info == "name"):     
            Sql_info.c.execute('SELECT id, name from code_component where trial_id = ?', (Sql_info.run_num,))
        #get type from code_component_id in code_component table
        elif (info == "type"):
            Sql_info.c.execute('SELECT id, type from code_component where trial_id = ?', (Sql_info.run_num,))
        #get mode from code_component_id in code_component table
        elif (info == "mode" ):
            Sql_info.c.execute('SELECT id, mode from code_component where trial_id = ?', (Sql_info.run_num,))
        #eval_id
        elif (info == "code_component_id"):
            Sql_info.c.execute('SELECT id, code_component_id from evaluation where trial_id = ?', (Sql_info.run_num,))
        elif (info == "repr"):
            Sql_info.c.execute('SELECT id, repr from evaluation where trial_id = ?', (Sql_info.run_num,))
        #get time from evaluation table
        elif(info == "checkpoint"):
            Sql_info.c.execute('SELECT id, checkpoint from evaluation where trial_id = ?', (Sql_info.run_num,))

        id_info = Sql_info.c.fetchall()
        id_info_pair = dict(id_info)
        return id_info_pair.get(id)

    def get_code_component_id_eval(self, evaluation_id):
        #get the code_component_id from evaluation_id in the evaluation table
        return Sql_info.get_basic_info(self, evaluation_id, "code_component_id")

    def get_value_eval(self, evaluation_id):
        #get repr(value) from evaluation table
        return Sql_info.get_basic_info(self, evaluation_id, "repr")

    def get_line_col_info(self, code_component_id, item):
        if (item == "last_char_column"):
            Sql_info.c.execute('SELECT id, last_char_column from code_component where trial_id = ?', (Sql_info.run_num,))
        elif (item == "last_char_line"):
            Sql_info.c.execute('SELECT id, last_char_line from code_component where trial_id = ?', (Sql_info.run_num,))
        elif (item == "first_char_column"):
            Sql_info.c.execute('SELECT id, first_char_column from code_component where trial_id = ?', (Sql_info.run_num,))
        elif (item == "first_char_line"):
            Sql_info.c.execute('SELECT id, first_char_line from code_component where trial_id = ?', (Sql_info.run_num,))
        id_line = Sql_info.c.fetchall()
        id_line_pair = dict(id_line)
        return id_line_pair.get(code_component_id)

    def get_environment_info(self, attribute_id_num):
        Sql_info.c.execute('SELECT value from environment_attr where id = ?', (attribute_id_num,))
        for element in Sql_info.c:
            envir = element
        return envir[0]

    def getOS(self):
        return Sql_info.get_environment_info(self, 1) + Sql_info.get_environment_info(self, 108)

    def get_script(self):
        Sql_info.c.execute('SELECT value from argument where trial_id = ? and id = ?', (Sql_info.run_num,1,))
        for element in Sql_info.c:
            script = element
        return script[0].strip("'")

    def get_script_time(self):
        Sql_info.c.execute('SELECT finish from trial where id = ?', (Sql_info.run_num,))
        for element in Sql_info.c:
            start = element
        return start[0]

    def get_prov_time(self):
        Sql_info.c.execute('SELECT timestamp from tag where trial_id = ?', (Sql_info.run_num,))
        for element in Sql_info.c:
            start = element
        return start[0]

    def get_total_elapsedTime(self):
        Sql_info.c.execute('SELECT start from trial where id = ?', (Sql_info.run_num,))
        for element in Sql_info.c:
            start = element
        startTime = start[0].split() 
        finishTime = Sql_info.get_script_time(self).split() 
        FMT = '%H:%M:%S.%f'
        tdelta = datetime.strptime(finishTime[1], FMT) - datetime.strptime(startTime[1], FMT)
        result = str(tdelta).replace('.', ' ').replace(':', ' ').split()
        time = ""
        if (result[0] != '0'):
            time = result[0]
            time +=":"
            time += result[1]
            time +=":"
            time += result[2]
            time +="."
            time += result[3]

        elif (result[1] != '00'):
            time += result[1]
            time +=":"
            time += result[2]
            time +="."
            time += result[3]
        elif (result[2] != '00'):
            time += result[2]
            time +="."
            time += result[3]
        elif (result[3] != '000000'):
            time += "0"
            time +="."
            time += result[3]
        return time

    def elpasedTime_timeStamp(self, elpasedTime):
        '''
        change elpasedTime in data nodes into timeStamp
        '''
        Sql_info.c.execute('SELECT start from trial where id = ?', (Sql_info.run_num,))
        for row in Sql_info.c:
            for element in row:
                tempStampParser = element.replace('.', ' ').replace(':', ' ').split() 
        elpasedTimeParser = str(elpasedTime).split(".")
        newMSecond = int(tempStampParser[-1]) + int(elpasedTimeParser[-1])
        newSecond = int(tempStampParser[-2]) + int(elpasedTimeParser[0])
        tempStampParser[-2] = str(newSecond)
        tempStampParser[-1] = str(newMSecond)
        return tempStampParser[0] + " " + tempStampParser[1] + ":" + tempStampParser[2] + ":" + tempStampParser[3] + "." + tempStampParser[4]


    def get_elapsedTime(self, code_component_id):
        '''
        get existing elapsedTime for procedure nodes, code-component -> code_block -> activation
        '''
        Sql_info.c.execute('SELECT id from code_block where trial_id = ?', (Sql_info.run_num,))
        code_block_id = []
        for row in Sql_info.c:
            for char in row:
                code_block_id.append(char)

        exist = False
        #check if this code_component has elapsedTime
        for element in code_block_id:
            if (code_component_id == element):
                exist = True

        if (exist == True):
            Sql_info.c.execute('SELECT start_checkpoint, code_block_id from activation where trial_id = ?', (Sql_info.run_num,))
            activation = []
            for row in Sql_info.c:
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


    def file_mode(self, name):

        '''
        if the file_access table exist, get file information
        '''
        #get the count of tables with the name
        Sql_info.c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='file_access' ''')
        #if the count is 1, then table exists
        if Sql_info.c.fetchone()[0]==1:

            #get hash value from table
            temp = []
            Sql_info.c.execute('SELECT name, mode from file_access where trial_id = ?', (Sql_info.run_num,))
            for row in Sql_info.c:
                for char in row:
                    temp.append(char)

            name_mode_pair = {}

            i = 0
            while i <  len(temp)-1:
                name_mode_pair[temp[i]] = temp[i+1]
                i=i+2
            return name_mode_pair.get(name)

    def file_access_table(self,name):

        stripped_name = name.strip("'")

        '''
        if the file_access table exist, get file information
        '''
        #get the count of tables with the name
        #c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='file_access' ''')
        #if the count is 1, then table exists
        if (Sql_info.file_mode(self, stripped_name) == "rU"):

            #get hash value from table
            temp = []
            Sql_info.c.execute('SELECT name, content_hash_before from file_access where trial_id = ?', (Sql_info.run_num,))
            for row in Sql_info.c:
                for char in row:
                    temp.append(char)

            name_hash_pair = {}

            i = 0
            while i <  len(temp)-1:
                name_hash_pair[temp[i]] = temp[i+1]
                i=i+2
            return name_hash_pair.get(stripped_name)


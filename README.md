# provTranslateNow
Translates provenance collected by NoWorkflow into JSON provenance as used by end-to-end tools. Queries the SQLite database db.sqlite and converts the information to PROV_JSON format. The result is stored in the now.json which is in the same directory of the python script. It can be read by End-To-End tools such as DDG Explorer and provSummarizeR.

# Installation
Install noWorkflow version 2.0.0a0 (you should use python3):
1. Clone the noWorkflow repository:
```
git clone https://github.com/gems-uff/noworkflow.git
```
2. Get in the noworkflow directory:
```
cd noworkflow
```
3. Switch to branch 2.0-alpha:
```
git checkout 2.0-alpha
```
4. Install noWorkflow locally using pip
```
python3 -m pip install -e capture
```
5. Running the python script:
```
now run script.py 
```
Now you will get a .noworkflow folder in the the directory of your python script

# Usage
1. Clone the provTranslateNow repository:
```
cd clone https://github.com/End-to-end-provenance/provTranslateNow.git
```
2. Get in the provTranslateNow directory:
```
cd provTranslateNow
```
3. running the provTranslateNow
```
python3 translator.py
```
you will be ask to enter the path of your db.sqlite file and a trial id
example input:
```
Enter the path of your db.sqlite file: /Users/huiyunpeng/Desktop/demo/.noworkflow/db.sqlite
Enter the trial id: 6
```
Then a file now.json will be created in the directory of your python script. 
You can run this file in the provSummarizeR and DDG Explorer.



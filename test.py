import DB

# DB.Insert_User('id' , 'pw' , 'name')


# DB.Update_BeforeInfo('id' , None , None , None)

# DB.Update_BeforeInfo('id' , 10 , None , None)

import json

list_str = [50.1 , 10.1 , 20.6 , 70.7 , 60.4]
list2_str = [94.25, 32.78, 76.19, 57.31, 89.63]

# DB.Insert_Play('id' , '2025-05-29' , 'squart' , 10 , json.dumps(list_str))
# DB.Insert_Play('id' , '2025-05-29' , '런지' , 25 , json.dumps(list2_str))

json_str = json.dumps(DB.Get_BeforeInfo('id') ,ensure_ascii=False , indent= 4 )
print(json_str)
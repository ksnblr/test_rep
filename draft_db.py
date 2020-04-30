import pyodbc
import pandas as pd
output_file = open('../draft_db_output.txt','w')
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=pvdsql3sim;DATABASE=QAOpsReport;UID=xkk;PWD=Divi4851#;Trusted_Connection=yes')
cursor=conn.cursor()

def get_int_value(output,table_name,columnname,value):
    cursor.execute("select {} from {} where {} = \'{}\'".format(output,table_name,columnname,value))
    for row in cursor:
        return row[0]
        break

q = """
DELETE from app_ir_level
DELETE from scenario_irdetails
DELETE from function_irs
DELETE from role_ir_details
DELETE from scenario
DELETE from sim_fqa
DELETE from sim_fir
DELETE from sim_qaj
DELETE from simfunctions
"""
try:
    cursor.execute(q)
except Exception as e:
    output_file.write(e)
    output_file.write("deletion of data failed\n")
output_file.write("Deletion of data successful\n")

try:

    df = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\irs_21x.xlsx",header=None,sheet_name=1)
    df1 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\irs_20x.xlsx",header=None,sheet_name=1)
    df2 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\irs_19x.xlsx",header=None,sheet_name=1)
    df3 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\scenario_replay_data.xlsx",header=None,sheet_name=2)
    df4 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\function_data.xlsx",header=None,sheet_name=5)
    df5 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\IRS_data.xlsx",header=None,sheet_name=0)
    ir_detail=df.append([df1,df2,df3,df4,df5])
    ir_detail = ir_detail.fillna('NULL')
except Exception as e:
    output_file.write(e)
    output_file.write("Reading IR Data failed\n")

output_file.write("Adding Detailed IR Information\n")
try:
    for x,row in ir_detail.iterrows():
        if row[6] !='NULL':
            item_6= get_int_value("qa_service_id","qa_service","qa_service_name",row[6])
            params = (row[0],row[1],row[2],row[3],row[4],row[5],item_6)
            cursor.execute("{CALL add_ir_details(?,?,?,?,?,?,?)}",params)
        else:
            params = (row[0],row[1],row[2],row[3],row[4],row[5])
            cursor.execute("{CALL add_ir_details(?,?,?,?,?,?)}",params)

except Exception as e:
    output_file.write ("App IR Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("Detailed IR Update Successful\n")

output_file.write ("App IR update started\n")
try:

    df = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\irs_21x.xlsx",header=None,sheet_name=0)
    df1 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\irs_20x.xlsx",header=None,sheet_name=0)
    df2 = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\irs_19x.xlsx",header=None,sheet_name=0)
    role_app_ir = df.append([df1,df2])

    for x,row in role_app_ir.iterrows():
        item_1= get_int_value("app_id","simapps","app_trigram",row[0])
        item_2= get_int_value("ir_level_id","ir_level_lookup","ir_level",row[2])
        cursor.execute("INSERT INTO app_ir_level(app_id,ir_id,ir_level_id) VALUES (?,?,?)",item_1,row[1],item_2)

except Exception as e:
    output_file.write ("App IR Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("App IR update successfull\n")

output_file.write ("Scenario update started\n")
try:

    df_scen = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\scenario_replay_data.xlsx",header=None,sheet_name=0)
    for x,row in df_scen.iterrows():
        item_2= get_int_value("scenario_type_id","scenario_type","scenario_type",row[2])
        item_4= get_int_value("release_id","release","release_name",row[4])
        item_6= get_int_value("qa_service_id","qa_service","qa_service_name",row[6])
        item_7= get_int_value("status_id","status","test_status",row[7])
        item_8= get_int_value("platform_id","platform","platform_type",row[8])

        cursor.execute("INSERT INTO scenario(scenario_id,scenario_link,scenario_type_id,scenario_title,release_id,replay_link,qa_service_id,status_id,platform_id) VALUES (?,?,?,?,?,?,?,?,?)",row[0],row[1],item_2,
                  row[3],item_4,row[5],item_6,item_7,item_8)
except Exception as e:
    output_file.write ("Scenario Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("Scenario update successful\n")

output_file.write ("Scenario IR update started\n")

try:

    df_scen_ir = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\scenario_replay_data.xlsx",header=None,sheet_name=1)
    for x,row in df_scen_ir.iterrows():
        cursor.execute("INSERT INTO scenario_irdetails(scenario_id,ir_id) VALUES (?,?)",row[0],row[1])
except Exception as e:
    output_file.write ("Scenario IR Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("Scenario IR update successful\n")

output_file.write ("Function update started\n")
try:

    df_function = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\function_data.xlsx",header=None,sheet_name=0)
    for x,row in df_function.iterrows():
        item_1= get_int_value("release_id","release","release_name",row[1])
        item_3= get_int_value("qa_service_id","qa_service","qa_service_name",row[3])
        cursor.execute("INSERT INTO simfunctions(function_link,release_id,function_title,qa_service_id,fqa_qa_name) VALUES (?,?,?,?,?)",row[0],item_1,row[2],item_3,row[4])
except Exception as e:
    output_file.write ("Function Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("Function update successful\n")

output_file.write ("FQA update started\n")
try:
    df_fqa = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\function_data.xlsx",header=None,sheet_name=1)
    df_fqa = df_fqa.fillna('None')
    for x,row in df_fqa.iterrows():
        item_0= get_int_value("function_id","simfunctions","function_link",row[0])
        if (row[3] == 'None'):
            cursor.execute("INSERT INTO sim_fqa(function_id,fqa_progress,fqa_status,fqa_remarks) VALUES (?,?,?,?)",item_0,int(row[1]),row[2],str(row[4]))
        else:
            cursor.execute("INSERT INTO sim_fqa(function_id,fqa_progress,fqa_status,fqa_td,fqa_remarks) VALUES (?,?,?,?,?)",item_0,int(row[1]),row[2],row[3],str(row[4]))
except Exception as e:
    output_file.write ("FQA Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("FQA Update successful\n")


output_file.write ("FIR Update started\n")
try:
    df_fir = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\function_data.xlsx",header=None,sheet_name=2)
    df_fir = df_fir.fillna('None')
    for x,row in df_fir.iterrows():
        item_0= get_int_value("function_id","simfunctions","function_link",row[0])
        if (row[3] == 'None'):
            cursor.execute("INSERT INTO sim_fir(function_id,fir_progress,fir_status,fir_remarks) VALUES (?,?,?,?)",item_0,int(row[1]),row[2],str(row[4]))
        else:
            cursor.execute("INSERT INTO sim_fir(function_id,fir_progress,fir_status,fir_td,fir_remarks) VALUES (?,?,?,?,?)",item_0,int(row[1]),row[2],row[3],str(row[4]))
except Exception as e:
    output_file.write ("FIR Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("FIR Update successful\n")

output_file.write ("QAJ Update started\n")
try:
    df_qaj = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\function_data.xlsx",header=None,sheet_name=3)
    for x,row in df_qaj.iterrows():
        item_0= get_int_value("function_id","simfunctions","function_link",row[0])
        cursor.execute("INSERT INTO sim_qaj(function_id,qaj_status) VALUES (?,?)",item_0,row[1])
except Exception as e:
    output_file.write ("QAJ Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("QAJ Update successful\n")
output_file.write ("Function IR Update Started\n")

try:
    df_fun_irs = pd.read_excel("\\filer1sim\\q\\qa\\work_areas\\xkk\\do_not_delete\\21x_role_irs_source\\function_data.xlsx",header=None,sheet_name=4)
    for x,row in df_fun_irs.iterrows():
        item_0= get_int_value("function_id","simfunctions","function_link",row[0])
        cursor.execute("INSERT INTO function_irs(function_id,ir_id) VALUES (?,?)",item_0,row[1])
except Exception as e:
    output_file.write ("Function IR Update Failed\n")
    output_file.write ("Error occured while executing row-{}\n".format(row))
    output_file.write(e)

output_file.write ("Function IR update successful\n")
output_file.write ("All updates successful\n")
cursor.commit()
output_file.close()

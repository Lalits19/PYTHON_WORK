
import os
import cx_Oracle
import time
import string
import logging
import csv
from datetime import date, timedelta
import datetime
run_date = date.today()
logger = logging.getLogger('ERROR-MODULE1')
os.chdir(r'C:\Metro-Link Work\INIT PROJECT\CQC TEMPLATE\PYTHON_WORK')
LOG_FILE_NAME = "LOGFILE"+'\CQC_LOG_FILE_'+ run_date.strftime('%Y%m%d')+'.log'
hdlr = logging.FileHandler(LOG_FILE_NAME)
formatter = logging.Formatter('%(name)s | %(asctime)s |  %(levelname)s: %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)
#logger.debug('PROGRAM STARTED')

conn_str = u'ods/ods@WARE'
conn = cx_Oracle.connect(conn_str)
v_test = 1
logger = logging.getLogger('DATA-BASE CONNECTION')
hdlr = logging.FileHandler(LOG_FILE_NAME)
formatter = logging.Formatter('%(name)s | %(asctime)s |  %(levelname)s: %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)
for v_test in range(1, 10):
   duplicate=[]
   #print("start",duplicate)
   v_corp_id = v_test
   cur = conn.cursor()
   test = "SELECT CORP_ID,1,0,A.CC_NO, CASE WHEN LENGTH(CORP_ID) > 4 THEN  TO_CHAR(CORP_ID) ELSE  TO_CHAR(CORP_ID)||'0'  END ,CORP_ID,CASE WHEN LENGTH(REPLACE(CARD_HOLDER_NAME,' ','')) IS NULL  THEN 'UNASSIGNED' ELSE SUBSTR(CARD_HOLDER_NAME,1,20) END  ,CASE WHEN LENGTH(REPLACE(CARD_HOLDER_NAME,' ','')) IS NULL  THEN 'UNASSIGNED ' ELSE CARD_HOLDER_NAME END   ,CASE WHEN LENGTH(REPLACE(CARD_HOLDER_NAME,' ','')) IS NULL  THEN 'UNASSIGNED ' ELSE CARD_HOLDER_NAME  END  ,DECODE(TT_ID,101015,101011,101215,101211,101615,101611,101715,101711,TT_ID) ,ORIGIN,DESTINATION,CASE  WHEN SUBSTR(TO_CHAR(B.TT_ID),1,4) ='1012' THEN '31' WHEN SUBSTR(TO_CHAR(B.TT_ID),1,4) ='1017' THEN '5' WHEN SUBSTR(TO_CHAR(B.TT_ID),1,4) ='1016' THEN '1'  ELSE '1' END, 1 FROM CMSE_CQC_CARD A , CMSE_CQC_CARD_ORDER B , CQC_CARD_SN C WHERE A.CC_NO = B.CC_NO   AND A.CORP_ID = C.ORG_ID AND  STATUS  <> 4 AND SR_NO = "
   test  =test+str(v_corp_id) +  " order by 1,2,3,4,5"
   res = cur.execute(test)

   data = res.fetchall()
   #logger.info('DATA RETREIEVED')
   for rec in data:
      lalit = [rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8], rec[9], rec[10], rec[11], rec[12],rec[13]]
      duplicate.append(lalit)

   file_name ='C:\Metro-Link Work\INIT PROJECT\CQC TEMPLATE\PYTHON_WORK\CSV_FILE\\CQC_TEMPLATE_'+str(v_corp_id)+'.csv'
   print(v_test,"  " ,"CQC_TEMPLATE_"+str(v_corp_id)+".csv","  " ,cur.rowcount)
   MESSAGE = str(v_test)  + " CQC_TEMPLATE_" + str(v_corp_id) + ".csv" + str(cur.rowcount)
   logger.info(MESSAGE)
   with open(file_name, 'w', newline='') as newcsvfile2:
    fieldnames = ['BATCH_NUMBER','CARD_TICKET','FARE_CATEGORY','CC_NO','PIN','CORP_ID','PARTICIPATION_IDENT','CC_FIRST_NAME','CC_LAST_NAME','TT_ID','ORIGIN','DESTINATION','TICKETS_PER_BC','ISAUTORENEWENABLED']
    writer = csv.DictWriter(newcsvfile2, fieldnames=fieldnames)
    writer.writeheader()
   for test in duplicate:
      with open(file_name, 'a', newline='', encoding='utf-8')as newcsvfile2:
       writer = csv.writer(newcsvfile2)
       writer.writerow(test)
   #print("Next Loop",v_test)


cur.close()
conn.close()
#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import fsspec
from google.cloud import bigquery
from datetime import datetime
from datetime import date as d
import os
from google.oauth2 import service_account
from google.cloud import storage
from datetime import timedelta


# In[2]:


dates = d.today()
times = datetime.now()

key_path='C:/Users/roop.sidhu_spicemone/Downloads/roop-sidhu.json'

def main():
    
    
    date = d.today()-timedelta(1)
    current_date5 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(1)
    current_date2 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(2)
    current_date3 = date.strftime('%d-%m-%Y')

    date = date.today()
    current_date4 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(1)
    current_date6 = date.strftime('%Y%m%d')

    current_date = date.today()-timedelta(1)

    date = date.today()
    current_year = date.strftime('%Y')

    date = date.today()
    current_month = date.strftime('%m')

    date = date.today()-timedelta(1)
    current_day = date.strftime('%d')

    credentials = service_account.Credentials.from_service_account_file(key_path)
    project_id = 'spicemoney-dwh'
    
    client = bigquery.Client(credentials= credentials, project=project_id, location='asia-south1')

    #Specifying the path of the external file
    file_path = [str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/YBL_Account Statement/YBL_BS_'
                 +str(current_date6)+'_001481400000330/YBL_BS_'+str(current_date6)+'_001481400000330.csv']

    
    #Defining the columns of DB log table
    schema = [{'name':'row_number','type':'STRING'},
                {'name':'transaction_date','type':'TIMESTAMP'},
                {'name':'cod_account_number','type':'INTEGER'},
                {'name':'narration','type':'STRING'},
                {'name':'value_date','type':'DATE'},
                {'name':'cheque_number','type':'STRING'},
                {'name':'drcr_flag','type':'STRING'},
                {'name':'amount','type':'FLOAT'},
                {'name':'date_post','type':'DATE'},
                {'name':'running_balance','type':'FLOAT'},
                {'name':'urn','type':'STRING'},
                {'name':'bank_reference_number','type':'STRING'},
                {'name':'txns_id','type':'STRING'},
                {'name':'rrn_num','type':'STRING'},
                {'name':'mobile_num','type':'STRING'},
                {'name':'aggent_id','type':'STRING'}]
                
    #Specifying the header column            
    header_list = ['row_number','transaction_date','cod_account_number','narration','value_date','cheque_number','drcr_flag','amount','date_post','running_balance','urn','bank_reference_number','txns_id','rrn_num','mobile_num','aggent_id']
    
    # Reading data from CSV to dataframe
    df = pd.read_csv('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/YBL_Account Statement/YBL_BS_'+str(current_date6)+'_001481400000330/YBL_BS_'+str(current_date6)+'_001481400000330.CSV', skiprows=1, names=header_list, parse_dates = (['value_date', 'date_post', 'transaction_date']), low_memory=False,storage_options={"token": key_path} )
   
    df.drop(('row_number'), axis=1, inplace=True)

    df['value_date'] = df['value_date'].dt.date
    df['date_post'] = df['date_post'].dt.date
    
    newdf = df["narration"].str.split("/",expand = True)
      
    df['txns_id']=newdf[0]
    df['rrn_num']=newdf[1]
    df['mobile_num']=newdf[2]
    df['aggent_id']=newdf[3]
  
    filtereddf=df.loc[df['txns_id']=="211070869"]
   
    filtereddf.to_gbq(destination_table='sm_recon.wallet_ybl_bank_statement_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema,credentials=credentials)
  #  df.to_gbq(destination_table='prod_sm_recon.prod_wallet_ybl_bank_statement_log', project_id='spicemoney-dwh', if_exists='append' , table_schema = schema,credentials=credentials)
    
    
    #fetching data and forming table from FTR,ECOLLECT & YBL external file
    sql_query="""
     select ECOLLECT_REQUEST_ID,FTR_UNIQUE_IDENTIFICATION_NO,YBL_RRN_NUM, Ecollect_TRANSFER_UNIQUE_NO,TRANSFER_TYPE,CLIENT_ID,PAYMENT_STATUS,coalesce(SUM_OF_TRANSFER_AMT,0) as ECOLLECT_DETAIL_AMT,coalesce(FTR_AMOUNT,0) as FTR_AMOUNT ,coalesce(YBL_AMOUNT,0) as YBL_AMOUNT,coalesce(FTR_AMOUNT,0)-coalesce(SUM_OF_TRANSFER_AMT,0) as DIFF_ECOLLECT_VS_SPICE_FTR,coalesce(YBL_AMOUNT,0)-coalesce(SUM_OF_TRANSFER_AMT,0) as DIFF_ECOLLECT_VS_YBL from
    (
    select 
    t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
    t2.retailer_id AS CLIENT_ID,
    sum(t1.amount_transferred) as FTR_AMOUNT    
    FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
    JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
    where t1.comments IN ('Wallet Recharge by E-Collection','HAPPY LOAN DISBURSEMENT AMOUNT MANUAL REVERSAL') and 
    DATE(t1.transfer_date) =  @date
    GROUP BY t1.UNIQUE_IDENTIFICATION_NO,CLIENT_ID) AS ftr_result
    FULL OUTER JOIN
    (
    select 
    a.REQUEST_ID as ECOLLECT_REQUEST_ID,
    a.client_id as ecollect_client_id,
    a.TRANSFER_UNIQUE_NO as Ecollect_TRANSFER_UNIQUE_NO,
    IFNULL(sum(a.transfer_amt), 0 )  as SUM_OF_TRANSFER_AMT,
    a.TRANSFER_TYPE as TRANSFER_TYPE,
    a.status as PAYMENT_STATUS
    from `spicemoney-dwh.prod_dwh.ec_notify_request` a where DATE(a.transfer_timestamp) = @date
     and status='CREDITED'
    GROUP BY request_id,TRANSFER_UNIQUE_NO,TRANSFER_TYPE,ecollect_client_id,status
    )as ecollect_data ON 
    ftr_result.FTR_UNIQUE_IDENTIFICATION_NO=ecollect_data.ECOLLECT_REQUEST_ID
    FULL OUTER JOIN
    (
    select 
    sum(amount) as YBL_AMOUNT,
    aggent_id,
    rrn_num as YBL_RRN_NUM
    from `spicemoney-dwh.sm_recon.ra_ybl_bank_statement` a where DATE(value_date) = @date
    GROUP BY YBL_RRN_NUM,aggent_id
    )as ybl_data
    ON ybl_data.YBL_RRN_NUM=ecollect_data. Ecollect_TRANSFER_UNIQUE_NO
 
    
    """
    
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        
#    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
   # query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    
    
    
    #yes bank summary
    sql_query="""
    select Transaction_Date,Narrative,Debit_Amount,Credit_Amount from 
    (select DATE(transaction_date) as debit_date,sum(amount) as Debit_Amount from `spicemoney-dwh.sm_recon.wallet_ybl_bank_statement_log`
    where DATE(transaction_date)  = @date and drcr_flag='D' GROUP BY transaction_date,txns_id) as debit_data
    FULL  JOIN 
    (select DATE(transaction_date) as Transaction_Date,txns_id as Narrative,sum(amount) as Credit_Amount from `spicemoney-dwh.sm_recon.wallet_ybl_bank_statement_log`
    where DATE(transaction_date)  = @date and drcr_flag='C' GROUP BY transaction_date,txns_id) as credit_data  
    ON credit_data.Transaction_Date=debit_data.debit_date
    """
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_ybl_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        
#    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ybl_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
   # query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("wallet made")
    

    
    #Limit and MTD Format
    sql_query="""
    select Max_Unique_Identification_No, Min_Unique_Identification_No,Max_Amount,Min_Amount,maximum_Transfer_Type,"Ecollect Detail Report" as Report,"Yes bank" as Aggregator,Payment_Status from 
(Select Max_Unique_Identification_No, Min_Unique_Identification_No,Max_Amount,Min_Amount,maximum_Transfer_Type,"Ecollect Detail Report" as Report,"Yes bank" as Aggregator,Payment_Status,
DENSE_RANK() over(
            PARTITION BY maximum_Transfer_Type order by Max_Unique_Identification_No,Min_Unique_Identification_No
        ) type_rank 
from
(
  select t2.transfer_unique_no as Max_Unique_Identification_No ,t3.transfer_unique_no as Min_Unique_Identification_No, Max_Amount,Min_Amount,  maximum_Transfer_Type,minimum_Transfer_Type,Payment_Status from
(
 select  Max_Amount,Min_Amount,  maximum_Transfer_Type,minimum_Transfer_Type,maximum_value.Payment_Status as Payment_Status from
(select max(transfer_amt) as Max_Amount,transfer_type as maximum_Transfer_Type,status as Payment_Status
    from `spicemoney-dwh.prod_dwh.ec_notify_request` a where DATE(a.transfer_timestamp) =@date and status='CREDITED'
    group by transfer_type,status
    ) as maximum_value
JOIN   
(select min(transfer_amt) as Min_Amount,transfer_type as minimum_Transfer_Type
    from `spicemoney-dwh.prod_dwh.ec_notify_request` a where DATE(a.transfer_timestamp) =@date and status='CREDITED'
    group by transfer_type  ) as minimum_value
ON maximum_value.maximum_Transfer_Type=minimum_value.minimum_Transfer_Type
) as t1
JOIN
(
    select transfer_unique_no,transfer_amt,transfer_type
    from  `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date and status='CREDITED' 
) as t2
ON t1.maximum_Transfer_Type=t2.transfer_type and 
t1.Max_Amount=t2.transfer_amt
JOIN
(
    select transfer_unique_no,transfer_amt,transfer_type
    from  `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date and status='CREDITED' 
) as t3
ON t1.minimum_Transfer_Type=t3.transfer_type and 
t1.Min_Amount=t3.transfer_amt
)
)
WHERE 
        type_rank=1
    
    """
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_ybl_limit_mtd_format', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        
#    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ybl_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
   # query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("wallet made")


    sql_query="""
    select Transaction_Date,Txn_Count_As_Per_FTR_Revoke,Sum_Of_Amount_As_Per_Yes_Bank_DRCR_FLAG_C,Sum_Of_Amount_As_Per_FTR_Revoke_Wallet_Recharge_By_Ecoll,Sum_Of_Amount_As_Per_FTR_Revoke_Happy_Loan_Disbursement_Amt,Sum_Of_Amount_As_Per_FTR_Revoke_Wallet_Recharge_By_Ecoll_Reversal,Sum_Of_Amount_As_Per_FTR_Revoke_Happy_Loan_Disbursement_Amt_Manual_Reversal,Net_Amount_As_Per_FTR_Revoke,Sum_of_Amount_As_Per_Ecollect_logs_IMPS_Credited,Sum_of_Amount_As_Per_Ecollect_logs_NEFT_Credited,Sum_of_Amount_As_Per_Ecollect_logs_RTGS_Credited,Sum_of_Amount_As_Per_Ecollect_logs_UPI_Credited,Net_Amount_Ecollect_Detail_Logs,Sum_Of_Amount_As_Per_Yes_Bank_DRCR_FLAG_C-Net_Amount_As_Per_FTR_Revoke as  Diff_Yes_Bank_Amount_vs_FTR_Net_Amount ,
    Sum_Of_Amount_As_Per_Yes_Bank_DRCR_FLAG_C-Net_Amount_Ecollect_Detail_Logs as  Diff_Yes_Bank_Amount_vs_Ecollect_Net_Amount  from
    (select 
        DATE(transfer_date) as Transaction_Date,
        count(*) as Txn_Count_As_Per_FTR_Revoke
        FROM spicemoney-dwh.prod_dwh.cme_wallet 
        where comments IN ('Wallet Recharge by E-Collection','Happy loan disbursement amount') and 
        DATE(transfer_date) = @date group by Transaction_Date
    ) as t1,
    (
    select sum(amount) as Sum_Of_Amount_As_Per_Yes_Bank_DRCR_FLAG_C from `sm_recon.wallet_ybl_bank_statement_log` where drcr_flag='C' and DATE(transaction_date) = "2022-08-07"
    ) as t2,
    (
        select 
        count(*) as  Sum_Of_Amount_As_Per_FTR_Revoke_Wallet_Recharge_By_Ecoll 
        FROM spicemoney-dwh.prod_dwh.cme_wallet 
        where comments ='Wallet Recharge by E-Collection' and 
        DATE(transfer_date) = @date
    ) as t3,
    (
        select 
        count(*) as  Sum_Of_Amount_As_Per_FTR_Revoke_Happy_Loan_Disbursement_Amt 
        FROM spicemoney-dwh.prod_dwh.cme_wallet 
        where comments ='Happy loan disbursement amount' and 
        DATE(transfer_date) = @date
    ) as t4,
    (
        select 
        sum(amount_transferred) as  Sum_Of_Amount_As_Per_FTR_Revoke_Wallet_Recharge_By_Ecoll_Reversal 
        FROM spicemoney-dwh.prod_dwh.cme_wallet 
        where comments ='Wallet Recharge by E-Collection Reversal' and 
        DATE(transfer_date) = @date
    ) as t5,
    (
        select 
        sum(amount_transferred) as  Sum_Of_Amount_As_Per_FTR_Revoke_Happy_Loan_Disbursement_Amt_Manual_Reversal 
        FROM spicemoney-dwh.prod_dwh.cme_wallet 
        where comments ='HAPPY LOAN DISBURSEMENT AMOUNT MANUAL REVERSAL' and 
        DATE(transfer_date) = @date
    ) as t6,
    (
        select 
        sum(amount_transferred) as  Net_Amount_As_Per_FTR_Revoke  
        FROM spicemoney-dwh.prod_dwh.cme_wallet 
        where comments IN ('Wallet Recharge by E-Collection','HAPPY LOAN DISBURSEMENT AMOUNT MANUAL REVERSAL') and  
        DATE(transfer_date) = @date
    ) as t7,
    (
        select Sum_of_Amount_As_Per_Ecollect_logs_IMPS_Credited from(
        select 
        sum(transfer_amt) as  Sum_of_Amount_As_Per_Ecollect_logs_IMPS_Credited ,
        TRANSFER_TYPE as TRANSFER_TYPE,
        status as PAYMENT_STATUS
        from `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date
         and status='CREDITED' and TRANSFER_TYPE='IMPS' group by TRANSFER_TYPE,PAYMENT_STATUS)

    ) as t8,
    (
        select Sum_of_Amount_As_Per_Ecollect_logs_NEFT_Credited from(
        select 
        sum(transfer_amt) as  Sum_of_Amount_As_Per_Ecollect_logs_NEFT_Credited ,
        TRANSFER_TYPE as TRANSFER_TYPE,
        status as PAYMENT_STATUS
        from `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date
         and status='CREDITED' and TRANSFER_TYPE='NEFT' group by TRANSFER_TYPE,PAYMENT_STATUS)

    ) as t9,
    (
        select Sum_of_Amount_As_Per_Ecollect_logs_RTGS_Credited from(
        select 
        sum(transfer_amt) as  Sum_of_Amount_As_Per_Ecollect_logs_RTGS_Credited ,
        TRANSFER_TYPE as TRANSFER_TYPE,
        status as PAYMENT_STATUS
        from `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date
         and status='CREDITED' and TRANSFER_TYPE='RTGS' group by TRANSFER_TYPE,PAYMENT_STATUS)

    ) as t10,
    (
        select Sum_of_Amount_As_Per_Ecollect_logs_UPI_Credited from(
        select 
        sum(transfer_amt) as  Sum_of_Amount_As_Per_Ecollect_logs_UPI_Credited ,
        TRANSFER_TYPE as TRANSFER_TYPE,
        status as PAYMENT_STATUS
        from `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date
         and status='CREDITED' and TRANSFER_TYPE='UPI' group by TRANSFER_TYPE,PAYMENT_STATUS)

    ) as t11,
    (
        select  Net_Amount_Ecollect_Detail_Logs   from
        (
        select 
        sum(transfer_amt) as  Net_Amount_Ecollect_Detail_Logs ,
        status as PAYMENT_STATUS
        from `spicemoney-dwh.prod_dwh.ec_notify_request` where DATE(transfer_timestamp) =@date
         and status='CREDITED' group by PAYMENT_STATUS)

    ) as t12



    
    
    
    """

    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_ybl_recon_tracker_output', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
        
#    job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ybl_summary', write_disposition='WRITE_APPEND' ,  query_parameters=[
    #bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
   # query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("wallet made")
    

main()

print("wallet made")


# In[3]:


main()


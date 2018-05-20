# coding:utf-8

# Aurthor: Cai, Jiefei

# ---------- 1. URL ---------- #
# HOME_URL
ETL_HOME = r'D:\Study\Mycode\HOME'
ETL_Ctrl_Queue = r'D:\Study\Mycode\HOME\Queue'
ETL_Ctrl_Running = r'D:\Study\Mycode\HOME\Running'
ETL_Ctrl_Receive = r'D:\Study\Mycode\HOME\Receive'
ETL_Job_Model = r'D:\Study\Mycode\HOME\Model'
ETL_Job_Done = r'D:\Study\Mycode\HOME\Done'
ETL_Ctrl_JobInfo = r'D:\Study\Mycode\HOME\JobInfo'
ETL_Ctrl_Log = r'D:\Study\Mycode\HOME\Log\EtlLog'
ETL_Job_Log = r'D:\Study\Mycode\HOME\Log\EtlLog'
ETL_Ctrl_Dependency = r'D:\Study\Mycode\HOME\JobDependency'
ETL_Ctrl_Stream = r'D:\Study\Mycode\HOME\Stream'

# Model_File
Model_File_Shell = r'Model_Shell.sh'
Model_File_Python = r'Model_Python.py'

# Sleep_Seconds
Job_Receive_Check_Time = 30

# ---------- 2. System ---------- #
SystemList = ['S04',
              'S02',
              'S07']


# ---------- 3. Job ---------- #
PARALLEL_JOB_NUM = 2
MAX_PROCESS_NUM = 2
# coding:utf-8

import os,re,time
from datetime import datetime

import tools.Config as Config
from tools.LogUtil import LogUtil as LogUtil
from tools.FileUtil import FileUtil as FileUtil
from tools.DatetimeUtil import DatetimeUtil
import tools.ProcessUtil as ProcessUtil

_pool = None

def init():
    global _pool
    log = LogUtil()
    log.Clear_Ctrl_Log()
    log.Etl_Log('---------------------------------开始初始化---------------------------------')
    log.Etl_Log('开始清理调度')
    # ToDo 清理所有ETL_HOME下状态的文件
    # 创建进程池
    if _pool == None:
        _pool = ProcessUtil.CreateProcessPool()

def ExceptionExit(code=12):
    global _pool
    if _pool != None:
        _pool.close()
    exit(code)


def updateJob(Job,dt,Last_Stats):
    # Job:{'Name':
    # ,'dt':
    # ,'model':
    # ,'system':}
    # dt:20171025
    # Last_Stats:20171024
    try:
        InfoPath = Config.ETL_Ctrl_JobInfo + os.sep + Job['system'] + os.sep + Job['Name']
        if not os.path.exists(InfoPath):
            LogUtil.Etl_Log(r'作业信息不存在，请检查：'+InfoPath)
        f = open(InfoPath,'r')
        f_read = f.read()
        f.close()
        f_List = f_read.split(' ')
        f_write = '%s %s %s %s' %(dt,f_List[1],f_List[2],Last_Stats)
        f = open(InfoPath, 'w')
        f.write(f_write)
        f.close()
    except Exception as e:
        print "执行updateJob失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.updateJob')
        ExceptionExit()
    finally:
        if 'f' in vars():
            f.close()


def checkRunning(Job,tx_dt):
    # Job:{'Name':
    # ,'dt':
    # ,'model':
    # ,'system':}
    # tx_dt:20171025
    try:
        RunningList = os.listdir(Config.ETL_Ctrl_Running)
        TrueList = []
        # Running flag file's name like : S04_S04_COMP_20171024.ctl
        RegexStr = re.compile(r'^\w+?_\w+_\d{8}\.ctl$')
        msg = r"正在检查执行中的作业..."
        LogUtil.Etl_Log(msg)
        for i in RunningList:
            if RegexStr.match(i):
                TrueList.append(i)
        msg = r"正在执行作业数量：[%s]" %len(TrueList)
        LogUtil.Etl_Log(msg)
        if len(TrueList) < Config.PARALLEL_JOB_NUM:
            if checkDependency(Job,tx_dt):
                msg = r"检查完毕，可以继续执行作业"
                LogUtil.Etl_Log(msg)
                return True
            else:
                msg = r"检查完毕，依赖作业未完成"
                LogUtil.Etl_Log(msg)
                return False
        else:
            msg = r"检查完毕，并行已满"
            LogUtil.Etl_Log(msg)
            return False
    except Exception as e:
        print "执行checkRunning失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.checkRunning')
        ExceptionExit()

def checkDependency(Job,tx_dt):
    # Job:{'Name':
    # ,'dt':
    # ,'model':
    # ,'system':}
    # tx_dt:20171025
    try:
        if Job == None:
            return False
        dt = DatetimeUtil()
        if dt.DateAdd(Job['dt'],1) != tx_dt:
            return False
        if not os.path.exists(Config.ETL_Ctrl_Dependency + os.sep + Job['system'] + os.sep + Job['Name']):
            return True
        with open(Config.ETL_Ctrl_Dependency + os.sep + Job['system'] + os.sep + Job['Name']) as f:
            f_read = f.read()
            depdir = f_read.split('\n')
            IsOk = 1
            for i in depdir:
                [depsys,depjob] = i.split(' ')
                with open(Config.ETL_Ctrl_JobInfo + os.sep + depsys + os.sep + depjob) as a:
                    a_read = a.read()
                    [job_dt, job_stats, job_model, Last_job_st] = a_read.split(' ')
                    if not (job_stats.upper() != 'V' and Last_job_st.upper() == 'DONE' and datetime.strptime(job_dt,'%Y%m%d') >= datetime.strptime(tx_dt,'%Y%m%d')):
                        IsOk = 0
            if IsOk == 1:
                return True
            else:
                return False
    except Exception as e:
        print "执行checkDependency失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.checkDependency')
        ExceptionExit()

def GetJobInfo(etl_sys,jobName):
    # etl_sys:S04
    # jobName:S04_COMP
    ReJobInfo = {}
    try:
        sys_dir = Config.ETL_Ctrl_JobInfo + os.sep + etl_sys
        file_job = open(sys_dir + os.sep + jobName, 'r')
        file_job_read = file_job.read()
        file_job.close()
        if file_job_read.strip() == '':
            return False
        [job_dt, job_stats, job_model, Last_job_st] = file_job_read.split(' ')
        if job_stats.upper() != 'V':
            ReJobInfo = {
                'Name': jobName,
                'dt': job_dt,
                'model': job_model,
                'system': etl_sys
            }
        return ReJobInfo
    except Exception as e:
        print "执行GetJobInfo失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.GetJobInfo')
    finally:
        if 'file_job' in vars():
            file_job.close()



def GetJobStats():
    JobStats = {}
    try:
        msg = r"-----开始检查作业状态-----"
        #LogUtil.Etl_Log(msg)
        for etl_sys in Config.SystemList:
            sys_dir = Config.ETL_Ctrl_JobInfo + os.sep + etl_sys
            if not os.path.exists(sys_dir):
                continue
            job_list = os.listdir(sys_dir)
            job_info = {}
            for job in job_list:
                job_info[job] = GetJobInfo(etl_sys,job)
                if job_info[job] == False:
                    continue
            JobStats[etl_sys] = {
                'Jobs':job_info,
            }
        msg = r'-----检查作业状态完毕-----'
        #LogUtil.Etl_Log(msg)
        return JobStats
    except Exception as e:
        print "执行GetJobStats失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.GetJobStats')


def CheckQueue():
    # 如果是数据库，可以设置优先级之类的来做排序，只取前n个作业判断是否可以执行。而后cp到Receive目录
    # 这边第一版用的是文件来保存信息。优先级虽然也可以做，但是目测效率上依旧不会太好
    # 遂放弃治疗。无视效率。不考虑优先级。所有check后可以run的作业全部扔到Receive目录
    try:
        JobList = os.listdir(Config.ETL_Ctrl_Queue)
        reg = re.compile(r'^(\w+?)_(\w+)_(\d{8})\.ctl$')
        for i in JobList:
            if reg.match(i):
                (etl_sys, job, dt) = reg.findall(i)[0]
                Job = GetJobInfo(etl_sys,job)
                if checkDependency(Job,dt):
                    FileUtil.move(Config.ETL_Ctrl_Queue + os.sep + i,Config.ETL_Ctrl_Receive + os.sep + i)
                else:
                    msg = r'依赖未完成，继续等待:' + etl_sys + '_' + job
                    LogUtil.Etl_Log(msg)
    except Exception as e:
        print "执行CheckQueue失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.CheckQueue')

def StartJob():
    global _pool
    Receive = os.listdir(Config.ETL_Ctrl_Receive)
    Running = os.listdir(Config.ETL_Ctrl_Running)
    QueueLs = os.listdir(Config.ETL_Ctrl_Queue)
    # S04_S04_COMP_20171024.ctl
    reg = re.compile(r'^(\w+?)_(\w+)_(\d{8})\.ctl$')
    JobStats = GetJobStats()
    for i in Receive:
        if i in Running:
            msg = r"作业已经在执行中:%s" %i
            LogUtil.Etl_Log(msg)
            os.remove(Config.ETL_Ctrl_Receive + os.sep + i)
        if i in QueueLs:
            msg = r"作业已经在队列中:%s" %i
            LogUtil.Etl_Log(msg)
            os.remove(Config.ETL_Ctrl_Receive + os.sep + i)
        getList = reg.findall(i)[0]
        if getList != []:
            if getList[1] in JobStats[getList[0]]['Jobs']:
                if checkRunning(JobStats[getList[0]]['Jobs'][getList[1]],getList[2]):
                    FileUtil.move(Config.ETL_Ctrl_Receive + os.sep + i,Config.ETL_Ctrl_Running + os.sep + i)
                    arg = JobStats[getList[0]]['Jobs'][getList[1]]
                    _pool.apply_async(RunJob,(arg,i,))
                else:
                    FileUtil.move(Config.ETL_Ctrl_Receive + os.sep + i,Config.ETL_Ctrl_Queue + os.sep + i)


def RunJob(Job,CtlName):
    # Job:{'Name':
    # ,'dt':
    # ,'model':
    # ,'system':}
    # CtlName:S04_S04_COMP_20171023.ctl
    reg = re.compile(r'^(\w+?)_(\w+)_(\d{8})\.ctl$')
    getList = reg.findall(CtlName)[0]
    msg = r"开始执行作业：%s" %(getList[1]+'_'+getList[2])
    print msg
    LogUtil.Etl_Log(msg)
    try:
        for i in xrange(3):
            time.sleep(10)
            print Job['Name'] + '  ' + str(i)
        FileUtil.move(Config.ETL_Ctrl_Running + os.sep + CtlName,Config.ETL_Job_Done + os.sep + CtlName)
    except Exception as e:
        updateJob(Job, CtlName[len(CtlName) - 12:-4], 'FAILED')
        LogUtil.Etl_Log_Exception(e, 'StartJob.RunJob:'+CtlName)
        ExceptionExit()
    msg = "作业执行完毕：%s %s" %(Job['Name'],CtlName[len(CtlName) - 12:-4])
    print msg
    LogUtil.Etl_Log(msg)
    updateJob(Job, CtlName[len(CtlName) - 12:-4], 'DONE')
    # ToDo 调起流作业
    touchStream(CtlName)


def touchStream(CtlName):
    # CtlName:S04_S04_COMP_20171023.ctl
    try:
        reg = re.compile(r'^(\w+?)_(\w+)_(\d{8})\.ctl$')
        (etl_sys,etl_job,dt) = reg.findall(CtlName)[0]
        streamDir = Config.ETL_Ctrl_Stream + os.sep + etl_sys + os.sep + etl_job
        if os.path.exists(streamDir):
            with open(streamDir,'r') as f:
                f_read = f.read()
                [st_sys,st_job] = f_read.split(' ')
                filename = '%s_%s_%s.ctl' %(st_sys,st_job,dt)
                path = Config.ETL_Ctrl_Queue + os.sep + filename
                FileUtil.touch(path)
        else:
            return False
    except Exception as e:
        print "执行touchStream失败"
        LogUtil.Etl_Log_Exception(e,'StartJob.touchStream')
        exit(12)




def main():
    global _pool
    init()
    while True:
        StartJob()
        CheckQueue()
        time.sleep(Config.Job_Receive_Check_Time)

if __name__ == '__main__':
    main()
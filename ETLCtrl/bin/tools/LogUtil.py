# coding:utf-8

# Aurthor: Cai, Jiefei

import Config
import os,shutil
from DatetimeUtil import DatetimeUtil

class LogUtil:
    #
    def __init__(self):
        if not os.path.isdir(Config.ETL_Ctrl_Log):
            os.mkdir(Config.ETL_Ctrl_Log)
        if not os.path.isdir(Config.ETL_Job_Log):
            os.mkdir(Config.ETL_Job_Log)

    @staticmethod
    def Etl_Log(msg = '',lvl = '',filename = ''):
        '''
        :param msg:Message
        :param lvl:Status
        :param filename:Log File Name
        '''
        try:
            dt = DatetimeUtil()
            if filename == '':
                filename = 'ETL_Ctrl_Log_%s.log' %dt.getToday()
            if lvl == '':
                lvl == '0'
            str_time = dt.getToday('YYYY-MM-DD HH:MM:SS')
            f = open(Config.ETL_Ctrl_Log + os.sep + filename,'a')
            line_msg = str(os.getpid()) + ' ' + str_time + ' ... ' + lvl + ' ' + msg + '\n'
            f.write(line_msg)
        except Exception as e:
            print '写入日志失败'
            print e
            exit(12)
        finally:
            if 'f' in vars():
                f.close()

    @staticmethod
    def Etl_Log_Exception(e,FunctionName):
        LogUtil.Etl_Log("程序执行失败无法继续，退出执行")
        lvl = e.__class__.__name__
        msg = str(e)
        LogUtil.Etl_Log(msg,lvl)
        msg = '异常方法：%s\n' %FunctionName
        LogUtil.Etl_Log(msg,lvl)
        exit(12)

    @staticmethod
    def Clear_Ctrl_Log(dt=''):
        try:
            if dt == '':
                print "清理所有ETL Ctrl日志"
                shutil.rmtree(Config.ETL_Ctrl_Log)
                os.mkdir(Config.ETL_Ctrl_Log)
            else:
                print "清理%s的日志"
                filename = 'ETL_Ctrl_Log_%s.log' %dt
                os.remove(Config.ETL_Ctrl_Log + os.sep + filename)
        except Exception as e:
            print e
            print '清理日志失败'
            exit(12)
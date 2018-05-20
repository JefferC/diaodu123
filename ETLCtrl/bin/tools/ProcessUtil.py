# coding:utf-8

# Aurthor: Cai, Jiefei

import multiprocessing
import Config

_pool = None

def CreateProcessPool():
    global _pool
    if _pool is None:
        _pool = multiprocessing.Pool(Config.MAX_PROCESS_NUM)
    return _pool

def ProcessClose():
    global _pool
    if _pool != None:
        try:
            _pool.close()
        except Exception as e:
            print e
            print 'Pool close failed'
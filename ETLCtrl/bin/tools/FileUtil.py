# coding:utf-8

import os,shutil

class FileUtil:

    @staticmethod
    def move(src,dst):
        if os.path.isfile(src) and not os.path.exists(dst):
            shutil.move(src,dst)
            return True
        else:
            return False

    @staticmethod
    def touch(path):
        if os.path.exists(path):
            os.remove(path)
        else:
            cmd = 'touch %s' %path
            os.system(cmd)
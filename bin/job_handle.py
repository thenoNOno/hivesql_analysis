#!/usr/bin/env python
# -*- coding: utf-8 -*
#date:2018-04-09
#author:qixuan.zhu

import os
import re
import numpy as np
import fire
from datetime import datetime

class carrier(object):

    def __init__(value):
        value=value

    def set_pattern(self,file):    # 从配置文件中获取待处理函数
        lines=[]
        f = open(file,encoding="utf-8")          # 返回一个文件对象
        for line in f:
            line = line.split("|")  # 正则分列
            line = [line[i] for i in (0,1)]
            lines.append(line)                 # 数据添加到lines数组
        f.close()
        return lines

    def analysis_job(self,file,pattern_file):            # 解析sql脚本
        pattern=[]
        pattern=self.set_pattern(pattern_file)
        print(pattern)
        lines=[]
        f=open(file,"r",encoding="utf-8")          # 返回一个文件对象
        for line in f:                               # 匹配source
            for row in pattern:
                line=line.replace(row[0],row[1])
            lines.append(line)
        f.close()
        return lines

    def scan_path(self,filepath,suffix):  # 根据路径与脚本后缀,扫描脚本
        filelist = []
        print("开始扫描:'{0}'".format(filepath))
        try:
            for filename in os.listdir(filepath):
                if os.path.isdir(filepath + "/" + filename):
                    filelist.extend(self.scan_path(filepath + "/" + filename, suffix))
                else:
                    if filename.endswith(suffix):
                        filelist.append((filepath + "/" + filename))
            return filelist
        except Exception as error:
            print("错误信息:",error)

class agent(object):

    def __init__(value):
        value=value

    def refresh_job(self,file,pattern_file):  # 解析并将结果写回脚本中
        c=carrier()
        lines = []
        lines=c.analysis_job(file,pattern_file)
        f = open(file,"w",encoding="utf-8")  # 写入文件
        for line in lines:
            line = re.sub("\n","",str(line))
            if not len(line) == 0:
                f.write(line+"\n")
        f.close()
        return lines

    def make_plan(self,filepath,suffix):  # 将需要解析的脚本放入todolist
        c = carrier()
        p = c.scan_path(filepath,suffix)
        todolist = []
        for file in p:
            todolist.append(file)
        return todolist

    def run(self,filepath,suffix,conf):  # 执行替换
        if conf=='''table''':
            conf='''./table.conf'''
        elif conf=='''func''':
            conf='''./func.conf'''
        else:
            raise ValueError()
        todolist = "./.todolist.plan"
        try:
            if os.path.exists(todolist):
                os.remove(todolist)     # 清除已有todolist文件,如果该文件被占用则说明上一次计划未完成,将抛出警告,中断执行
        except Exception as warning:
            print("警告信息:",warning)
            os._exit(0)
        p = self.make_plan(filepath,suffix)
        f = open(todolist,"a",encoding="utf-8")
        for line in p:          # 将待解析文件名写入todolist文件
            f.write(line+"\n")
        f.close()
        todo = open(todolist,"r",encoding="utf-8")
        for file in todo:       # 解析注解与脚本
            file = re.sub("\n","",file)
            self.refresh_job(file,conf)
            print(file)
        todo.close()

# #main方法执行
# def main():
#     filepath="e:/workspace/bigdata-dwh/code/dwd/user_profile/job"
#     suffix="sql"
# #    conf="func"
#     conf="table"
#     a=agent()
#     a.run(filepath,suffix,conf)

if __name__=='''__main__''':
#    main()
    fire.Fire(agent)
# 执行命令行
# python job_handle.py run "e:/workspace/bigdata-dwh/code/dwd/user_profile/job" "sql" "table"

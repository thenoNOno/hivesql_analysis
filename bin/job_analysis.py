#!/usr/bin/env python
# -*- coding: utf-8 -*
#date:2018-04-11
#author:qixuan.zhu

import os
import re
import numpy as np
import fire
from datetime import datetime

class carrier(object):

    def __init__(value):
        value=value

    def analysis_job(self,flag,clean_flag,pattern,sub,tag,file):            # 解析sql脚本
        lines = []
        f = open(file,"r",encoding="utf-8")          # 返回一个文件对象
        for line in f:                               # 匹配source
            if not re.match(flag,line,re.I):
                line = re.search(pattern,line,re.I)
                if line:
                    line = re.sub(sub,"|",line.group(),re.I)
                    line = tag+line
                    line = "--   #"+re.sub(" |\n","",str(line)).replace(', ','|')
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

    def refresh_job(self,file):  # 解析并将结果写回脚本中
        c=carrier()
        update_time = str(datetime.now())
        filename = re.sub(".*['/']","",file)
        flag = "--.*"
        clean_flag = "--.*?(@|#|/)"
        tag_source = "source"
        pattern_source = "(?i)(join|from)(\s+)[\w].*?[\s]"
        sub_source = "(?i)(join|from)[\s+]"
        tag_target = "target"
        pattern_target = "(?i)(insert.*?table)[\s*].*?[\s]"
        sub_target = "(?i)(insert.*?table)[\s*]"
        lines = []
        lines_tmp = []

        f = open(file,"r",encoding="utf-8")  # 清理历史标记
        for line in f:
            if not re.match(clean_flag,line,re.I):
                if not len(line) == 0:
                    lines.append(line)
        f.close()

        lines.append("--/**#"+update_time)  # 脚本解析开始
        source = c.analysis_job(flag,clean_flag,pattern_source,sub_source,tag_source,file)
        lines.extend(source)
        target = c.analysis_job(flag,clean_flag,pattern_target,sub_target,tag_target,file)
        lines.extend(target)
        lines.append("--**/\n")  # 脚本解析结束

        lines_tmp = lines       # 去重
        lines = list(set(lines_tmp))
        lines.sort(key=lines_tmp.index)

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

    def run(self,filepath,suffix):  # 解析并将结果放入metastore
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
            self.refresh_job(file)
            print(file)
        todo.close()

# #main方法执行
# def main():
    # filepath="e:/workspace/bigdata-dwh/code/dwd/user_profile/job"
    # suffix="sql"
    # a=agent()
    # a.run(filepath,suffix)

if __name__=='''__main__''':
#    main()
    fire.Fire(agent)
# 执行命令行
# python job_analysis.py run "e:/workspace/bigdata-dwh/code/dwd/user_profile/job" "sql"

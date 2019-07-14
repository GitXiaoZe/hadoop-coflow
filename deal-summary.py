#!/bin/env/python3
import sys
import os
import numpy

class Mapper:
    def __init__(self):
        self.id = -1
        self.input_size = -1
        self.output_size = -1
        self.cpu_time = -1
        self.task_start_time = -1
        self.task_map_phase_start_time = -1
        self.task_map_phase_finish_time = -1
        self.task_sort_phase_finish_time = -1
        self.task_finish_time = -1

        self.task_init_time = -1
        self.task_map_phase_time = -1
        self.task_sort_phase_time = -1
        self.task_fini_time = -1
        self.task_total_time = -1

    def setId(self, id):
        self.id = id

    def setInputSize(self, input_size):
        self.input_size = input_size

    def setOutputSize(self, output_size):
        self.output_size = output_size

    def setCpuTime(self, cpu_time):
        self.cpu_time = cpu_time

    def setTaskStartTime(self, task_start_time):
        self.task_start_time = task_start_time

    def setTaskMapPhaseStartTime(self, task_map_phase_start_time):
        self.task_map_phase_start_time = task_map_phase_start_time

    def setTaskMapPhaseFinishTime(self, task_map_phase_finish_time):
        self.task_map_phase_finish_time = task_map_phase_finish_time

    def setTaskSortPhaseFinishTime(self, task_sort_phase_finish_time):
        self.task_sort_phase_finish_time = task_sort_phase_finish_time

    def setTaskFinishTime(self, task_finish_time):
        self.task_finish_time = task_finish_time

    def calculate(self):
        self.task_init_time = self.task_map_phase_start_time - self.task_start_time
        self.task_map_phase_time = self.task_map_phase_finish_time - self.task_map_phase_start_time
        self.task_sort_phase_time = self.task_sort_phase_finish_time - self.task_map_phase_finish_time
        self.task_fini_time = self.task_finish_time - self.task_sort_phase_finish_time
        self.task_total_time = self.task_finish_time - self.task_start_time

    def toString(self):
        return str(self.id) + "\t" + str(self.input_size) + "\t" + str(self.output_size) + "\t" + \
               str(self.task_init_time) + "\t" + str(self.task_map_phase_time) + "\t" + str(self.task_sort_phase_time) \
               + "\t" + str(self.task_fini_time) + "\t" + str(self.task_total_time) + "\t" + str(self.cpu_time) + "\n"


class Reducer:
    def __init__(self):
        self.id = -1
        self.input_size = -1
        self.output_size = -1
        self.cpu_time = -1
        self.task_start_time = -1
        self.task_shuffle_phase_start_time = -1
        self.task_shuffle_phase_finish_time = -1
        self.task_sort_phase_finish_time = -1
        self.task_reduce_phase_finish_time = -1
        self.task_finish_time = -1

        self.task_init_time = -1
        self.task_shuffle_phase_time = -1
        self.task_sort_phase_time = -1
        self.task_reduce_phase_time = -1
        self.task_fini_time = -1
        self.task_total_time = -1

    def setId(self, id):
        self.id = id

    def setInputSize(self, input_size):
        self.input_size = input_size

    def setOutputSize(self, output_size):
        self.output_size = output_size

    def setCpuTime(self, cpu_time):
        self.cpu_time = cpu_time

    def setTaskStartTime(self, task_start_time):
        self.task_start_time = task_start_time

    def setTaskShufflePhaseStartTime(self, task_shuffle_phase_start_time):
        self.task_shuffle_phase_start_time = task_shuffle_phase_start_time

    def setTaskShufflePhaseFinishTime(self, task_shuffle_phase_finish_time):
        self.task_shuffle_phase_finish_time = task_shuffle_phase_finish_time

    def setTaskSortPhaseFinishTime(self, task_sort_phase_finish_time):
        self.task_sort_phase_finish_time = task_sort_phase_finish_time

    def setTaskReducePhaseFinishTime(self, task_reduce_phase_finish_time):
        self.task_reduce_phase_finish_time = task_reduce_phase_finish_time

    def setTaskFinishTime(self, task_finish_time):
        self.task_finish_time = task_finish_time

    def calculate(self):
        self.task_init_time = self.task_shuffle_phase_start_time - self.task_start_time
        self.task_shuffle_phase_time = self.task_shuffle_phase_finish_time - self.task_shuffle_phase_start_time
        self.task_sort_phase_time = self.task_sort_phase_finish_time - self.task_shuffle_phase_finish_time
        self.task_reduce_phase_time = self.task_reduce_phase_finish_time - self.task_sort_phase_finish_time
        self.task_fini_time = self.task_finish_time - self.task_reduce_phase_finish_time
        self.task_total_time = self.task_finish_time - self.task_start_time

    def toString(self):
        return str(self.id) + "\t" + str(self.input_size) + "\t" + str(self.output_size) + "\t" + \
               str(self.task_init_time) + "\t" + str(self.task_shuffle_phase_time) + "\t" + \
               str(self.task_sort_phase_time) + "\t" + str(self.task_reduce_phase_time) + "\t" + \
               str(self.task_fini_time) + "\t" + str(self.task_total_time) + "\t" + str(self.cpu_time) + "\n"

class TaskType:
    map_type = 0
    reduce_type = 1

class Prefix:
    attampt = ' Final Counters for attempt'
    map_input = 'HDFS: Number of bytes read'
    map_output = 'Map output materialized bytes'
    reduce_input = 'Reduce shuffle bytes'
    reduce_output = 'HDFS: Number of bytes written'
    cpu_time = 'CPU time spent (ms)'
    map_phase_start_time = ' The task map phase start time is'
    map_phase_finish_time = ' The task map phase finish time is'
    shuffle_phase_start_time = ' The task shuffle phase start time is '
    shuffle_phase_finish_time = ' The task shuffle phase finish time is '
    sort_phase_finish_time = ' The task sort phase finish time is '
    reduce_phase_finish_time = ' The task reduce phase finish time is '
    task_start_time = ' metrics system\'s start time is '
    task_finish_time = ' metrics system\'s finish time is '


for root, dirs, files in os.walk("templog/"):
    for file in files:
        if 'summary' not in file:
            continue
        mapper = list()
        reducer = list()
        thisfile = open(root+file, 'r')
        # out = open('application_1540033449643_0004_data.txt', 'w')
        lines = thisfile.readlines()
        temp = None
        for line in lines:
            # start with date
            if line.startswith('201'):
                splits = line.split(':')
                # find the map/reduce id
                if splits[3].startswith(Prefix.attampt):
                    if type(temp) == Mapper and temp is not None:
                        temp.calculate()
                        mapper.append(temp)
                    elif type(temp) == Reducer and temp is not None:
                        temp.calculate()
                        reducer.append(temp)
                    else:
                        pass
                    words = splits[3].split('_')
                    for i in range(0, len(words)):
                        if words[i] == 'm':
                            temp = Mapper()
                            temp.setId(int(words[i + 1]))
                            break
                        if words[i] == 'r':
                            temp = Reducer()
                            temp.setId(int(words[i + 1]))
                            break
                # find the timestamp in different phase
                elif splits[3].startswith(Prefix.map_phase_start_time):
                    words = splits[3].split(' ')
                    temp.setTaskMapPhaseStartTime(int(words[-1]))

                elif splits[3].startswith(Prefix.map_phase_finish_time):
                    words = splits[3].split(' ')
                    temp.setTaskMapPhaseFinishTime(int(words[-1]))

                elif splits[3].startswith(Prefix.sort_phase_finish_time):
                    words = splits[3].split(' ')
                    temp.setTaskSortPhaseFinishTime(int(words[-1]))

                elif splits[3].startswith(Prefix.shuffle_phase_start_time):
                    words = splits[3].split(' ')
                    temp.setTaskShufflePhaseStartTime(int(words[-1]))

                elif splits[3].startswith(Prefix.shuffle_phase_finish_time):
                    words = splits[3].split(' ')
                    temp.setTaskShufflePhaseFinishTime(int(words[-1]))

                elif splits[3].startswith(Prefix.reduce_phase_finish_time):
                    words = splits[3].split(' ')
                    temp.setTaskReducePhaseFinishTime(int(words[-1]))

                elif splits[3].startswith(Prefix.task_start_time):
                    words = splits[3].split(' ')
                    temp.setTaskStartTime(int(words[-1]))

                elif splits[3].startswith(Prefix.task_finish_time):
                    words = splits[3].split(' ')
                    temp.setTaskFinishTime(int(words[-1]))
                else:
                    pass
            else:
                if type(temp) == Mapper:
                    splits = line.strip().split('=')
                    if splits[0] == Prefix.map_input:
                        temp.setInputSize(int(splits[1]))
                    elif splits[0] == Prefix.map_output:
                        temp.setOutputSize(int(splits[1]))
                    elif splits[0] == Prefix.cpu_time:
                        temp.setCpuTime(int(splits[1]))
                if type(temp) == Reducer:
                    splits = line.strip().split('=')
                    if splits[0] == Prefix.reduce_input:
                        temp.setInputSize(int(splits[1]))
                    elif splits[0] == Prefix.reduce_output:
                        temp.setOutputSize(int(splits[1]))
                    elif splits[0] == Prefix.cpu_time:
                        temp.setCpuTime(int(splits[1]))

        print(file)
        mapper.sort(key=lambda m: m.id)

        mapper_input_size = list()
        mapper_output_size = list()
        mapper_task_init_time = list()
        mapper_task_map_phase_time = list()
        mapper_task_sort_phase_time = list()
        mapper_task_fini_time = list()
        mapper_task_total_time = list()
        mapper_task_cpu_time = list()
        for m in mapper:
            mapper_input_size.append(m.input_size)
            mapper_output_size.append(m.output_size)
            mapper_task_init_time.append(m.task_init_time)
            mapper_task_map_phase_time.append(m.task_map_phase_time)
            mapper_task_sort_phase_time.append(m.task_sort_phase_time)
            mapper_task_fini_time.append(m.task_fini_time)
            mapper_task_total_time.append(m.task_total_time)
            mapper_task_cpu_time.append(m.cpu_time)

        # print('map mean: ')
        print(numpy.mean(mapper_input_size))
        print(numpy.mean(mapper_output_size))
        print(numpy.mean(mapper_task_init_time))
        print(numpy.mean(mapper_task_map_phase_time))
        print(numpy.mean(mapper_task_sort_phase_time))
        print(numpy.mean(mapper_task_fini_time))
        print(numpy.mean(mapper_task_total_time))
        print(numpy.mean(mapper_task_cpu_time))
        # print('map var: ')
        print(numpy.var(mapper_input_size))
        print(numpy.var(mapper_output_size))
        print(numpy.var(mapper_task_init_time))
        print(numpy.var(mapper_task_map_phase_time))
        print(numpy.var(mapper_task_sort_phase_time))
        print(numpy.var(mapper_task_fini_time))
        print(numpy.var(mapper_task_total_time))
        print(numpy.var(mapper_task_cpu_time))


        reducer_input_size = list()
        reducer_output_size = list()
        reducer_task_init_time = list()
        reducer_task_shuffle_phase_time = list()
        reducer_task_sort_phase_time = list()
        reducer_task_reduce_phase_time = list()
        reducer_task_fini_time = list()
        reducer_task_total_time = list()
        reducer_task_cpu_time = list()
        reducer.sort(key=lambda r: r.id)
        for r in reducer:
            reducer_input_size.append(r.input_size)
            reducer_output_size.append(r.output_size)
            reducer_task_init_time.append(r.task_init_time)
            reducer_task_shuffle_phase_time.append(r.task_shuffle_phase_time)
            reducer_task_sort_phase_time.append(r.task_sort_phase_time)
            reducer_task_reduce_phase_time.append(r.task_reduce_phase_time)
            reducer_task_fini_time.append(r.task_fini_time)
            reducer_task_total_time.append(r.task_total_time)
            reducer_task_cpu_time.append(r.cpu_time)

        # print('reduce mean: ')
        print(numpy.mean(reducer_input_size))
        print(numpy.mean(reducer_output_size))
        print(numpy.mean(reducer_task_init_time))
        print(numpy.mean(reducer_task_shuffle_phase_time))
        print(numpy.mean(reducer_task_sort_phase_time))
        print(numpy.mean(reducer_task_reduce_phase_time))
        print(numpy.mean(reducer_task_fini_time))
        print(numpy.mean(reducer_task_total_time))
        print(numpy.mean(reducer_task_cpu_time))
        # print('reduce var: ')
        print(numpy.var(reducer_input_size))
        print(numpy.var(reducer_output_size))
        print(numpy.var(reducer_task_init_time))
        print(numpy.var(reducer_task_shuffle_phase_time))
        print(numpy.var(reducer_task_sort_phase_time))
        print(numpy.var(reducer_task_reduce_phase_time))
        print(numpy.var(reducer_task_fini_time))
        print(numpy.var(reducer_task_total_time))
        print(numpy.var(reducer_task_cpu_time))

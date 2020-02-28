import threading
import queue
import random
import time

from generator import *


maxThreads = 20  # 指定线程数
maxTask = 41  # 指定任务数量


class Executer():

    '''
    指定需要执行的任务类
    传入一个任务类(继承至 Generate_task 重写 rask 方法, 写入执行任务), 实例化, 注册进队列, 在队列开启多线程

    '''
    def __init__(self, worker):
        '''
        :param worker: 指定任务类
        '''
        self.worker = worker

    def main(self,):
        # 切片任务 长度使用 range 的值
        pipeline = queue.Queue(maxThreads)
        # 总共有多少任务
        for _ in range(maxTask):
            pipeline.put(_)
            task = eval(self.worker)(_, pipeline, ())  # 指定 store, queue, data
            task.start()
        pipeline.join()
        print('调度结束')


if __name__ == '__main__':
    Executer('Executer_chinaVideo_task').main()
    # a = {
    #     'x': 1,
    #     'y': 2,
    #     'z': 3
    # }
    # b = {
    #     'w': 10,
    #     'x': 11,
    #     'y': 2
    # }
    # print(set(a.keys()) - set(b.keys()))
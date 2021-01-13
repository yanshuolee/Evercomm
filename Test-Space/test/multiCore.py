from multiprocessing import Pool, Process
from threading import Thread
import os
import time
import threading

def f(x, y):
    pid = os.getpid() 
    print(x, pid)
    if x == 1:
        time.sleep(3)
    elif x== 2:
        time.sleep(6)
    ans = x*x*x*y*y
    print(f"ans of {x} is {ans}")
    return ans

def loop():
    for i in 10000:
        if i == 9990:
            print(9990)

def sp(sec):
    print(sec, threading.get_ident(), os.getpid() )
    time.sleep(sec)
    return        

def foo():
    # with Pool(5) as p:
    #     print(p.map(f, [[1, 5],[2, 5],[3, 5]]))
    
    # pool = Pool(4)
    # pool.map(f, [[1, 5],[2, 5],[3, 5]])

    # ========================================
    # try:
    #     a = Process(target=f, args=(1,5))
    #     b = Process(target=f, args=(2,5))
    #     c = Process(target=f, args=(3,5))
    #     d = Process(target=loop)

    #     a.start()
    #     b.start()
    #     c.start()
    #     d.start()
    #     print("Running while waiting.")
        
    # finally:
    #     a.join()
    #     b.join()
    #     c.join()
    #     d.join()
    #     print("Here")


    # ========================================

    try:
        # a = Process(target=sp, args=(8,))
        # b = Process(target=sp, args=(5,))
        a = Thread(target=sp, args=(8,))
        b = Thread(target=sp, args=(5,))

        a.start()
        b.start()

        print("Running while waiting.")

        print("a")
        s = time.time()
        a.join(3)
        print("wait a in ", time.time()-s)
        print("b")
        b.join()
        
    finally:
        print("Here")

foo()
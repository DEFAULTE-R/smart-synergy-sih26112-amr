from edge_runtime import MultiProcessEdgeDemo
import time
if __name__ == '__main__':
 d=MultiProcessEdgeDemo(); d.configure('stress',0.25,10); d.start(); end=time.time()+10
 while time.time()<end: d.poll(); time.sleep(.05)
 print(d.state()); d.stop()

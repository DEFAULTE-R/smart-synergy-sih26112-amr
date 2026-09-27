from edge_runtime import MultiProcessEdgeDemo
import time

def run(scenario, drop):
    d=MultiProcessEdgeDemo(); print('START',scenario,flush=True); d.configure(scenario,drop,10); d.start()
    end=time.time()+8
    while time.time()<end:
        d.poll(); time.sleep(.05)
    print(d.run_for(1),flush=True); d.stop()

if __name__ == '__main__':
    run('normal',0)
    run('blockage',0)
    run('comm_loss',0.35)

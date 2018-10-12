import pandas as pd
import os,sys,subprocess,time,traceback
from distributed import Client

def RCMES(t, fid):
    ''' Run RCMES code on files specified by ids
    @param t: task ID. Usefule when calling this function from Docker services
    @param id: file ID
    @return: Dictionary of benchmark results (e.g., start and end times) for processing specified files
    '''
    
    data_start=time.time()
    code = subprocess.call([sys.executable, '../run_RCMES.py', fid])  # Analyze specified configuration file
    if code:
            #errored.append(fid)
            data_end=-1     # Specific data analysis failed. Thus, record end time as -1
    else:
            data_end=time.time()
    return {'task':t,'data':fid,'start':data_start,'end':data_end}        # Benchmark analysis time for specific data configuration file

try:
        total_start=time.time()         # Total task start time in sec
        time_bench=[]                   # Record time steps for current task
        conf_f=pd.read_csv(sys.argv[1]) # Load input file containing IDs and paths for configuration file
        t=int(os.environ['t'])-1        # Task ID as defined by environment variable 't'. t is used to synchronize data access among containers. It should be noted that 't' environment values start from 1, not 0. This is why t is decremented by 1
        repl=int(os.environ['repl'])    # Number of container replicas (e.g., Docker swarm replicas)
        ids=conf_f.loc[conf_f['id']%repl==t]    # Extract paths for configuration fiels that are going to be analyzed by current container
        fout="/data/output/CORDEX/analysis/"+str(t)+".csv"
        print(fout)
        
        # Process parallel tasks on dask cluster if given
        # If not cluster specified, then use Docker Swarm (default)
        if os.environ['dask_cluster']:
            dask_cluster=os.environ['dask_cluster']
            client=Client(dask_cluster)
            res=[client.submit(RCMES,t,fid) for fid in ids['path']]
            time_bench.append(client.gather(res))
        else:   # Default processing on Docker swarm
            time_bench.append(RCMES(t, fid) for fid in ids['path'])
        
        total_end=time.time()           # Total task end time in sec
        df=pd.DataFrame(time_bench)
        df[['task','data','start','end']].to_csv(fout,index=False)      # Record time benchmark for current task in csv
        '''with open(flog,'w') as flog_f:
                for item in errored:
                        flog_f.write(item)
        '''
except:
        print('t='+str(t))
        print('repl='+str(repl))
        traceback.print_exc()

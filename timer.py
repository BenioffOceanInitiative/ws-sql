import time

n_jobs = 6780

t_beg = time.time()
for i_job in range(n_jobs):
  t_now = time.time()
  t_eta = t_beg + (t_now - t_beg) / (i_job + 1) * n_jobs
  time.ctime(t_eta)
  print(i)
  

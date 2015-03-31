#TYPE(=TABLE_DUMP2) | TIMESTAMP | ?(=B) | IP | AS | PREFIX | FROM TO(ASPATH) | ORIGIN(=IGP) | NEXT-HOP |
import matplotlib.pyplot as plt
import math
import numpy
import sys
import threading
import time

class Map(threading.Thread):
  def __init__(self, tid, records):
    threading.Thread.__init__(self)
    self.tid = tid
    self.records = records
    self.retAS_Pfx = {}
    self.retAS_Nbr = {}

  def run(self):
    f = 0
    for item in self.records:
      if '{' in item:
      	continue
      if ':' in item:
      	continue
      rawdata = item.split('|')
      prefix = rawdata[5]
      mask = int(prefix.split('/')[1])
      path = rawdata[6].split(' ')
      pathlen = len(path)

      self.retAS_Pfx[(int(path[-1]), prefix)] = True
      for i in range(pathlen):          #Q3
        if i - 1 in range(pathlen) and int(path[i]) != int(path[i - 1]):
            self.retAS_Nbr[(int(path[i]), int(path[i - 1]))] = True
            self.retAS_Nbr[(int(path[i - 1]), int(path[i]))] = True
      
  def getretAS_Pfx(self):
    return self.retAS_Pfx

  def getretAS_Nbr(self):
    return self.retAS_Nbr

class Reduce():
  def __init__(self):
    self.ASlist = {}
    self.Pfxlist = {}
    self.AS_Pfx = {}
    self.AS_Nbr = {}
    self.pfx_distrib = {}
    self.ip_distrib = {}
    self.deg_distrib = {}
    self.deg_pfx = {}
    self.m_deg_pfx = {}

  def run(self, mapAS_Pfx, mapAs_Nbr):
    for pfx in mapAS_Pfx:
      self.ASlist[pfx[0]] = True      #Q1.1
      self.Pfxlist[pfx[1]] = True     #Q1.2

      if not self.AS_Pfx.get(pfx[0]): #Q2
        self.AS_Pfx[pfx[0]] = {pfx[1] : True}
      else:
        self.AS_Pfx[pfx[0]][pfx[1]] = True

    for nbr in mapAs_Nbr:
      if not self.AS_Nbr.get(nbr[0]): #Q3
        self.AS_Nbr[nbr[0]] = {nbr[1] : True}
      else:
        self.AS_Nbr[nbr[0]][nbr[1]] = True
  
  def getAS_Nbr(self):
    return self.AS_Nbr

  def getAS_Number(self):           #Q1.1
    return len(self.ASlist.keys())

  def getPfx_Number(self):          #Q1.2
    return len(self.Pfxlist.keys())

  def getPfx_distrib(self):         #Q2
    for AS in self.ASlist:
      pfx_number = len(self.AS_Pfx[AS].keys()) if self.AS_Pfx.get(AS) else 0
      if not self.pfx_distrib.get(pfx_number):
        self.pfx_distrib[pfx_number] = 0
      self.pfx_distrib[pfx_number] += 1
    return self.pfx_distrib

  def getIp_distrib(self):
    for AS in self.ASlist:
      ip_namespace = sum(map(lambda x: 2 ** (32 - int(x.split('/')[1])), self.AS_Pfx[AS].keys())) if self.AS_Pfx.get(AS) else 0
      if not self.ip_distrib.get(ip_namespace):
        self.ip_distrib[ip_namespace] = 0
      self.ip_distrib[ip_namespace] += 1
    return self.ip_distrib

  def getDeg_distrib(self):         #Q3
    for AS in self.ASlist:
      degree = len(self.AS_Nbr[AS].keys()) if self.AS_Nbr.get(AS) else 0
      if not self.deg_distrib.get(degree):
        self.deg_distrib[degree] = 0
      self.deg_distrib[degree] += 1
    return self.deg_distrib

  def getDegPfx(self):              #Q4.1
    for AS in self.ASlist:
      degree = len(self.AS_Nbr[AS].keys()) if self.AS_Nbr.get(AS) else 0
      pfx_number = len(self.AS_Pfx[AS].keys()) if self.AS_Pfx.get(AS) else 0
      self.deg_pfx[(degree, pfx_number)] = True
    return self.deg_pfx

  def getMeanDegPfx(self):          #Q4.2
    for AS in self.ASlist:
      degree = len(self.AS_Nbr[AS].keys()) if self.AS_Nbr.get(AS) else 0
      pfx_number = len(self.AS_Pfx[AS].keys()) if self.AS_Pfx.get(AS) else 0
      if not self.m_deg_pfx.get(math.ceil(math.log(degree))):
        self.m_deg_pfx[math.ceil(math.log(degree))] = []
      self.m_deg_pfx[math.ceil(math.log(degree))].append(pfx_number)
    for deg in self.m_deg_pfx:
      self.m_deg_pfx[deg] = sum(self.m_deg_pfx[deg]) / len(self.m_deg_pfx[deg])
    return self.m_deg_pfx


def drawplot(rawdata, x_axis, y_axis, name):
  rawdata.sort()
  x = []
  y = []
  for i in rawdata:
    x.append(i[0])
    y.append(i[1])
  plt.title(name)
  plt.xlabel(x_axis)
  plt.ylabel(y_axis)
  plt.scatter(x, y)
  plt.show()

def mapreduce():
  with open(sys.argv[1],'r') as fp:
    lines = fp.readlines()
  starttime = time.clock()
  tM = 18
  t = len(lines)
  print 'TOTAL:', t
  record = []
  mapThreads = []
  for i in range(tM):
    record.append(lines[t / tM * i:t / tM * (i + 1) if t - t / tM * (i + 1) >= tM else t])
  print 'GENERATE DATA SETS'
  for i in range(tM):
    mapThreads.append(Map(i, record[i]))
    mapThreads[i].setDaemon(True)
    mapThreads[i].start()

  flag = 0
  while flag != (1 << tM) - 1:
    for i in range(tM):
      if not mapThreads[i].isAlive():
        flag |= 1 << i
  print 'MAP THREADS FINISH'

  reduce = Reduce()
  for i in range(tM):
    reduce.run(mapThreads[i].getretAS_Pfx(), mapThreads[i].getretAS_Nbr())
  print 'REDUCE FINISH'

  print 'Q1.1:', reduce.getAS_Number()
  print 'Q1.2:', reduce.getPfx_Number()
  pfx_distrib = reduce.getPfx_distrib()
  ip_distrib = reduce.getIp_distrib()
  deg_distrib = reduce.getDeg_distrib()
  deg_pfx = reduce.getDegPfx()
  m_deg_pfx = reduce.getMeanDegPfx()
  as_nbr = reduce.getAS_Nbr()
    
  AS1 = 9737
  print 'FIND P2C AS FOR AS#', AS1
  for AS2 in as_nbr[AS1].keys():
    if AS1 != AS2:
      if len(as_nbr[AS1].keys()) - len(as_nbr[AS2].keys()) > 1:
        print 'P2C:', AS1, AS2

  print 'FIND C2P AS FOR AS#', AS1
  for AS2 in as_nbr[AS1].keys():
    if AS1 != AS2:
      if len(as_nbr[AS2].keys()) - len(as_nbr[AS1].keys()) > 1:
        print 'C2P:', AS1, AS2

  print 'FIND P2P AS FOR AS#', AS1
  for AS2 in as_nbr[AS1].keys():
    if AS1 != AS2:
      if abs(len(as_nbr[AS1].keys()) - len(as_nbr[AS2].keys())) <= 1:
        continue
      print 'P2P:', AS1, AS2

  endtime = time.clock()
  print 'RUNTIME:', endtime - starttime
  
  drawplot(pfx_distrib.items(), 'PREFIX', '#AS','PREFIX-#AS')             #Q2.1
  drawplot(map(lambda t: (math.log(t[0]), math.log(t[1])), pfx_distrib.items()), 'PREFIX', '#AS','PREFIX-#AS')             #Q2.1

  drawplot(ip_distrib.items(), 'IP_NAMESPACE', '#AS','IP_NAMESPACE-#AS')  #Q2.2
  drawplot(map(lambda t: (math.log(t[0]), math.log(t[1])), ip_distrib.items()), 'IP_NAMESPACE', '#AS','IP_NAMESPACE-#AS')  #Q2.2
  
  drawplot(deg_distrib.items(), 'DEGREE', '#AS', 'DEGREE-#AS')            #Q3
  drawplot(map(lambda t: (math.log(t[0]), math.log(t[1])), deg_distrib.items()), 'DEGREE', '#AS', 'DEGREE-#AS')            #Q3

  drawplot(deg_pfx.keys(), 'DEGREE', '#PREFIX', 'DEGREE-#PREFIX')         #Q4
  drawplot(m_deg_pfx.items(), 'DEGREE', '#PREFIX', 'DEGREE-#PREFIX')      #Q4
if __name__=='__main__':
  mapreduce()
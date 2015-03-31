##Description
Analysis bgpdump data

output:
- how many AS(Q1.1)
- how many prefix (Q1.2)
- Prefix - #AS plot
- Prefix - #AS plot(log(x), log(y))
- IPNameSpace - #AS plot
- IPNameSpace - #AS plot(log(x), log(y))
- Degree of AS - #AS plot
- Degree of AS - #AS plot(log(x), log(y))
- Degree of AS - #Prefix plot
- Degree of AS - #Prefix plot(ceiling(log(x)), mean(y))
- Find P2C AS of AS#9737
- Find C2P AS of AS#9737
- Find P2P AS of AS#9737

##Feature
- MapReduce model
- Multi-Threads 

##Usage
`$ bgpdump rib.********.****.bz2 -m > bgpdump-output.txt`

`$ python bgp.py bgpdump-output.txt`

you can get `rib.********.****.bz2` from [http://archive.routeview.org](http://archive.routeview.org)
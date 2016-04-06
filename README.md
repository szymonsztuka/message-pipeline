# Radar
Application to monitor messages send via pipeline of tcp and ssh endpoints.

## Topology

* Pipeline is defined by 'command' property, pipeline definies order of starting and stoping in steps nodes:
command=(e1)({c1,c2}({b1,b2}(a))(d))(f1)

* Tap is a source of files (steps) to be send down to the pipe is defined by *.input property. Source of data is optional then pipeline runs once only.
a.input=dir_path

* Radar creates topology based on 'command' and '*.input' properties
INFO radar.topology.TopologyBuilder - Creating tap with steps sources {a=build/resources/main/data/input} with pipeline:
{c1, c2}
|------->{b1, b2}
|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|------->{a}
|------->{d}

* Tap collects all files (steps) 
INFO radar.topology.Tap - Resolving tap with 3 steps from a.input=build\resources\main\data\input with pipeline ({LongJvmProcess, LongJvmProcess}({SocketReader, SocketReader}({SocketWriter}))({ShortJvmProcess}))

* Radar runs topology instance - tap sends each file down to the pipeline as a separate step
INFO radar.topology.Tap - Running tap with 3 steps with pipeline ({LongJvmProcess, LongJvmProcess}({SocketReader, SocketReader}({SocketWriter}))({ShortJvmProcess}))

## Remarks
For radar-samples Gradle 'build' plugin needs to be run before 'application' plugin. 
'build' adds additional jars executies from within sample app.
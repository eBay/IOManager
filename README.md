# IOManager
[![Conan Build](https://github.com/eBay/IOManager/actions/workflows/merge_build.yml/badge.svg?branch=master)](https://github.com/eBay/IOManager/actions/workflows/merge_build.yml)
[![CodeCov](https://codecov.io/gh/eBay/IOManager/branch/master/graph/badge.svg)](https://codecov.io/gh/eBay/IOManager)

IOManager tries to bridge the gap in existing async framework to build full async networked database/storage/keyvalue storage. Motivation 
and design are explained below.

## Motivation
Typical storage or database applications if it intends to do things in async manner, need some sort of multiplexing of multiple interfaces. 
The popular event libraries such as libevent, grpc iomanager all provides multiplexing of multiple socket and network connections. 
Once a thread pulls the data or command from network, it typically transfers the ownership of the data to a threadpool to do any IO 
activity. The IO activity will typically be using conventional system call APIs done synchronously.However passing to different threadpool 
to do IO activity does cost CPU cycles because of context switching and unbalanced memory access. 

Alternative to this model would be to "run_to_complete", wherein the thread which pulls the data/cmd from network will be the one
executing as well. This approach would certainly improve the performance, however, it will not scale if the operation it is going to
be performed are predominantly blocking in nature, example: IO activity. Hence the entire operation has to be async in nature. Till
recently there are several restrictions on doing async IO on Linux. Possibly because of this, there aren't many libraries which multiplexes
the socket and IO completion, in-order to achieve full asynchronous application. This threading model along with async non-blocking
operations significantly improve the performance.

Even on run_to_completion model there are 2 broad classifications.

a) **Interrupt mode multiplexing**: In this mode, all devices are multiplexed by handful of threads and sleep in the kernel to wait for
some event (either incoming command, response, IO completion, message, timer event) etc.. and those events generate an interrupt to the
thread, which then waktes up and pull the event. Typical iomanagers would have the number of threads little flexible and all threads
would listen to any events

b) **Tight Loop Polling**: In this mode, threads instead of sleeping, runs in a tight loop and keep polling for events. This coupled
with programming model of sharding the requests (or endpoints or connections) to a specific thread, results in unmatched performance
per core. 

While tight loop polling, provides near unmatched performance on heavy load it burns CPU as number of threads increase. This model needs
significant programming changes and in practice sharding the requests to a specific thread is not possible.

IOManager tries to bridge these 2 programming models (by allowing to run side-by-side) and thus allowing practical applications could
use performance as well as flexibility of scaling number of threads and also provide all event libraries functionality.

## SPDK
Intel has come up with SPDK (Storage Performance Development Kit) which provides the above mentioned tight loop polling model. They
provide specific storage protocol servers (called targets) for different transports - NVMe over TCP, iSCSI, NVMe over RDMA, NVMe over FC
etc and very high performing direct user space NVMe drivers which support non-blocking IO. In addition SPDK have low level libraries 
which one could potentially integrate to multiplex generic sockets as well. The requirement to use SPDK is application need to work
with that threading model exclusively.

As mentioned in motivation section, it provides near unmatched performance per core, but every thread that application creates which needs
to do IO, is expected to run on its own CPU core, running tight loop. As a result overall CPU usage of the system would be equal to
the number of cores. This would clearly be impractical. 

IOManager does a hybrid design, which exposes SPDK and also has equivalent EPoll (Interrupt based polling) in one application side-by-side
and transparently pass the IOs to appropraite thread and get the completion.

## Design
![IOManager Design](IOManager.jpg)

### IO Reactors
IO Reactor is the event loop which waits for events from various devices or end points. Since it runs in a loop until it is
explicitly asked to be broken, it occupies one system thread. 

There are 2 types of reactors 
1) **Tight Loop Reactor** (a.k.a SPDK Reactor) which runs in a tight loop in poll mode without sleeping.
2) **Interrupt Loop Reactor** (a.k.a Epoll Reactor) which runs in a epoll loop waiting for any events from kernel

Apart from this classification, there are 2 more classification
1) **Worker Reactor:** Reactors which are managed by the IOManager itself. It is typically started during the application startup and runs
   until IOManager is stopped. IOManager could increase or decrease this type of reactors (though that code is still in works)
2) **User Reactor:** Reactors loops which are explicitly started by application and are joined the list of reactors later. Application
   can start its own system thread and join to become user reactor later and then relinquish its status of being a reactor later.

An application could have either tight_loop_worker_reactors or interrupt_loop_worker_reactors, but not both. In addition, an application
can start interrupt_loop_user_reactor. At this point, we don't see any use cases for tight_loop_user_reactor.

NOTE: SPDK code base itself has a similar concept called "reactors". However, IOManager has implemented its own reactor and integrates with
SPDK at lower layer than reactors - called *thread_lib*, because of its needs to work with epoll and other type of reactors.

### IO Threads
IO Thread is a soft thread or lightweight thread runs inside a reactor. This is not the same as system thread. A reactor could run 
multiple IO Threads. Each IOthread is guaranteed to run in only 1 reactor (thus a system thread). The devices are schedule and added to 
the io thread for listening events and hence it could be moved around to different reactors dynamically. All devices that
are added to the iothread can interact with each other without taking any locks, irrespective of which reactor/system thread it is 
currently running. 

NOTE: The light weight thread is directly mapped one to one with spdk_thread (which is the same concept. In fact IO Thread concept is 
inspired from spdk_thread). 

NOTE: Moving io threads across is still work in progress.

In case of Interrupt loop reactor type (non-SPDK Reactors), there is only IO Thread per reactor and it can be interchangeably called epoll
reactor. This is because such concept of light weight thread is of not much use for epoll reactors.

### IO Devices
IO Device represents all the devices that IOManager will listen to and notifies of any events. At present, it supports File, Block Device, 
Event Fd, Generic linux file descriptors, Spdk bdevs (aio, nvme), NVMeOF (Qpairs)

## Build
To build (assuming a recent version of conan package manager is installed)
```
   $ ./prepare.sh # this will export some recipes to the conan cache
   $ conan create . <user>/<channel>
```

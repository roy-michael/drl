# DRL
Distributed Rate Limiter assigment, implementation

### Building
Note that the Go (golang) sdk must be installed for building the drl server binary, on the build machine.
In order to build the binary issue the following the command project directory:
```go build``` 

### Running
The drl binary does not need any special dependencies for runtime, and it's default parameters can be customized with the following:  
#### Parameters:
```Usage of ./drl:
     -addr string
       	address to listen on (default "localhost:8000")
     -maxAllowed int
       	the maximum number of allowed requests per minute (default 500)
     -members string
       	addresses of other cluster members
```

### Notes
1. In order to cope with the high network latency between the different regions, I decided to have an asynchronous 
procedure for synchronizing the request counters between the cluster nodes. The rate count is verified on each 
node based on a local store of all IDs request count, without the need to have a network call.
2. Having the request counters sync asynchronously has its cost of losing the accuracy of the counter verification. In 
some situations we can have higher rate count than allowed, due to late remote count sync cycles. we can reduce the error 
rate by reducing the duration between cycles.
3. The RPC protocol between the Load Balancer and the DRL is done with a single endpoint, for verifying if a client request
should be proxied based on the allowed count in the 1 minute time frame. It was implemented with JSON over http for the 
sake of brevity, but it should be implemented over faster protocols (websockets/tcp) and better message formats (binary) 
to reduce latency even more.
4. There is an additional endpoint to be used by the DRL nodes to sync the counter information between themselves
5. The counter sync and reset relies on a common facility such as NTP to sync the node's machine clocks
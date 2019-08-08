# Baudtime Command line Operation Tool

./console -h 127.0.0.1 -p 8089                                                
127.0.0.1:8089> slaveof 127.0.0.1 8088                                        
OK                                                                            
127.0.0.1:8089> info                                                          
Shard: 607eb5e0-7a08-11e8-9007-00ffd6453d6d                                   
IP: 127.0.0.1                                                                 
Port: 8089                                                                    
DiskFree: 442                                                                 
IDC:                                                                          
127.0.0.1:8089> writepoint ops{app="baudtime",idc="langfang"} 600             
127.0.0.1:8089> writepoint ops{app="baudtime",idc="langfang"} 701                       
127.0.0.1:8089> instantqry ops{app="baudtime",idc="langfang"}                   
{
        "resultType": "vector",
        "result": [
                {
                        "metric": {
                                "__name__": "ops",
                                "app": "baudtime",
                                "idc": "langfang"
                        },
                        "value": [
                                1530109426.124,
                                "701"
                        ]
                }
        ]
}
#### TODO:
- [ ] add ping command
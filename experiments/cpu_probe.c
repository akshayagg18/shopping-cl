#define _GNU_SOURCE
#include <sched.h>
#include <stdio.h>
#include <unistd.h>
int main(){cpu_set_t m;CPU_ZERO(&m);if(sched_getaffinity(0,sizeof m,&m))return 1;printf("uid=%u cpus=",getuid());for(int i=0;i<CPU_SETSIZE;i++)if(CPU_ISSET(i,&m))printf("%d,",i);puts("");return 0;}

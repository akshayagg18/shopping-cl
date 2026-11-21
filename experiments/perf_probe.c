#include <linux/perf_event.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
int main(){struct perf_event_attr a;memset(&a,0,sizeof a);a.type=PERF_TYPE_HARDWARE;a.size=sizeof a;a.config=PERF_COUNT_HW_CPU_CYCLES;a.exclude_kernel=1;a.exclude_hv=1;int fd=syscall(__NR_perf_event_open,&a,0,-1,-1,0);if(fd<0){perror("perf_event_open");return 1;}close(fd);return 0;}

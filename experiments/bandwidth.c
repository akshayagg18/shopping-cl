#define _GNU_SOURCE
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
double now(){ struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec*1e-9; }
int main(int argc,char**argv){size_t total=0; __m256i acc=_mm256_setzero_si256();double t=now(); for(int a=1;a<argc;a++){int f=open(argv[a],O_RDONLY);struct stat st;fstat(f,&st);size_t n=st.st_size;if(n){unsigned char*p=mmap(0,n,PROT_READ,MAP_PRIVATE|MAP_POPULATE,f,0);if(p==MAP_FAILED)return 1;size_t i=0;for(;i+32<=n;i+=32)acc=_mm256_xor_si256(acc,_mm256_loadu_si256((void*)(p+i)));for(;i<n;i++)total+=p[i]&1;munmap(p,n);}total+=n;close(f);}printf("{\"seconds\":%.9f,\"checksum\":%llu,\"bytes_plus_tail_checksum\":%zu}\n",now()-t,(unsigned long long)(_mm256_extract_epi64(acc,0)^_mm256_extract_epi64(acc,1)^_mm256_extract_epi64(acc,2)^_mm256_extract_epi64(acc,3)),total);}

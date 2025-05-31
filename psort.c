/* Created by Ryan Martinez
 * Tested on WSL - Ubuntu
 * Output of lscpu:
 *Architecture:                       x86_64
CPU op-mode(s):                     32-bit, 64-bit
Address sizes:                      48 bits physical, 48 bits virtual
Byte Order:                         Little Endian
CPU(s):                             12
On-line CPU(s) list:                0-11
Vendor ID:                          AuthenticAMD
Model name:                         AMD Ryzen 5 5600 6-Core Processor
CPU family:                         25
Model:                              33
Thread(s) per core:                 2
Core(s) per socket:                 6
Socket(s):                          1
Stepping:                           2
BogoMIPS:                           6986.95
Flags:                              fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl tsc_reliable nonstop_tsc cpuid extd_apicid pni pclmulqdq ssse3 fma cx16 sse4_1 sse4_2 movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy svm cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw topoext perfctr_core ssbd ibrs ibpb stibp vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec xgetbv1 xsaves clzero xsaveerptr arat npt nrip_save tsc_scale vmcb_clean flushbyasid decodeassists pausefilter pfthreshold v_vmsave_vmload umip vaes vpclmulqdq rdpid fsrm
Virtualization:                     AMD-V
Hypervisor vendor:                  Microsoft
Virtualization type:                full
L1d cache:                          192 KiB (6 instances)
L1i cache:                          192 KiB (6 instances)
L2 cache:                           3 MiB (6 instances)
L3 cache:                           32 MiB (1 instance)
Vulnerability Gather data sampling: Not affected
Vulnerability Itlb multihit:        Not affected
Vulnerability L1tf:                 Not affected
Vulnerability Mds:                  Not affected
Vulnerability Meltdown:             Not affected
Vulnerability Mmio stale data:      Not affected
Vulnerability Retbleed:             Not affected
Vulnerability Spec rstack overflow: Mitigation; safe RET, no microcode
Vulnerability Spec store bypass:    Mitigation; Speculative Store Bypass disabled via prctl and seccomp
Vulnerability Spectre v1:           Mitigation; usercopy/swapgs barriers and __user pointer sanitization
Vulnerability Spectre v2:           Mitigation; Retpolines, IBPB conditional, IBRS_FW, STIBP always-on, RSB filling, PBRSB-eIBRS Not affected
Vulnerability Srbds:                Not affected
Vulnerability Tsx async abort:      Not affected
 *
 */


#define _GNU_SOURCE
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h> 
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <sched.h>
#include <sys/time.h>
#define RECORD_DATA_ARRAY_SIZE 124 // size in bytes of the array that contains the data associated with a Record. 
// Record size = 128 bytes, key size = 4 bytes, payload size = Record Size - Key Size = 124 Bytes. 
#define RECORD_SIZE 128
// backlog or TODO
// - implement merge sort -- complete -- 
//  ] Figure out how to adapt merge sort to sort based on the keys. -- in progress -- 
// - implement file opening, reading, writing. -- complete -- 
//  ] remember to use fseek(3) after write and read operations (it is good practice)
//  ] dump input file to stdout -- complete -- 
//  ] dump input file to output file == complete== 
// - create record structs -- complete -- 
// - implement functionality for creating new structs on the fly, and adding them to an array. -- complete  -- 
//  ] create for-loop that loops through the buffer, writing a record to the recordArray -- complete -- 
//  ] need to add these structs to a global array or another data structure so I have something  
// - implement creating threads
// - implement a queue to manage work -- in progress --
//  ] modify queue to work with project needs // i think it works?
//    ] need to be able to work with "tasks", the queue is keeping track of units of work that need to be completed.
//    create a new struct, call it "task_t". It will have the section of the array that needs to be sorted,
//    and the status, open, taken. 
//    -------- THE PLAN ----------------
//    The plan is to have the main thread create boundaries on the array. 
//    This will be done by doing the math on the mergeSort part on Merge Sort.
//    When it splits the array, these bounds are saved into Task structs and these structs are added onto the queue.//    A task struct has the following members:
//    - status: boolean [open, taken] 
//    - leftBoundary: int 
//    - rightBoundary: int
//    A critical question remains, how granular should we make these tasks?
//    Do we precompute the number of records that will need to be processed? This is certainly doable. 
//      - We need to take how many bytes we have read and divide the size of a record. 
//        This gives us how many records we have. 
//      - From there, we can probably determine how many threads we need. Remember, we need to give each thread 
//        enough work so it can give us meaningful performance increases. (performance in terms of time to sort)
//
// - implement a means of chunking work. (maybe arises naturally from merge sort)
//KNOWN ISSUES
// - memory leaks with the queue. Nodes and tasks are not properly freed when items are dequeued and the queue is freed. 
// ] cause: improper memory freeing.  
// ] fix: create a DESTROY QUEUE method that walks through the queue and frees all memory associated with it
// 
//
// - out of order records at the tail end of the array. An incredibly small fraction of the records are being placed out of order at the tail end of the array only.  
// cause: unknown
// ] fix: unknown. 
//
// code for merge sort below here. 
// a lot of the code for merge sort is taken from GeeksForGeeks.com. Link: https://www.geeksforgeeks.org/c-program-for-merge-sort/
//
// structs
typedef struct{
    uint32_t key;
    uint8_t data[RECORD_DATA_ARRAY_SIZE];
     // length in bytes of a record. key = 4 + data[] = 124 = 128 
} Record;
typedef struct{
    //bool status; // 0 = open, 1 = taken. Well, maybe I don't need this. 
    // A task is on the queue if it needs to be processed. When it is claimed by a thread, it is dequeued,
    int leftBoundary;
    int rightBoundary;
}task_t;

// CONCURRENT DATA STRUCTURES (QUEUE)
typedef struct __node_t{
    task_t* task;
    struct __node_t* next;
} node_t;

typedef struct __queue_t {
    node_t* head;
    node_t* tail;
    pthread_mutex_t head_lock, tail_lock;
    pthread_cond_t conditionVariable;
} queue_t;

typedef struct{
    int threadID;
    Record* array;
    queue_t* queue;
}args_t;


    
// CONCURRENT DATA STRUCTURES (QUEUE)
int initializeQueue(queue_t* q){
    // tmp->task struct leaks. Free appropriately (consider a DESTROY QUEUE method that frees all nodes on the queue and their tasks)
    node_t* tmp = (node_t*)malloc(sizeof(node_t));
    tmp->task = (task_t*) malloc(sizeof(task_t));

    tmp->next = NULL;
    q->head = q->tail = tmp;
    int rc = 0;
    rc = pthread_mutex_init(&q->head_lock, NULL);
    if( rc != 0){
     printf("failed to initialize a lock\n");
     return -1;
    }
    rc = pthread_cond_init(&q->conditionVariable, NULL); 
  
    if( rc != 0){
     printf("failed to initialize a condition variable\n");
     return -1;
    }
    rc = pthread_mutex_init(&q->tail_lock, NULL);
    
    if( rc != 0){
     printf("failed to initialize a lock\n");
     return -1;
    }
    return 0;
}

void enqueue(queue_t* q, task_t* value){
    // nodes on the queue are not freed appropriately TODO create a DESTROYQUEUE method that frees all nodes and tasks associated 
    node_t *tmp = malloc(sizeof(node_t));
    if(tmp == NULL){
        printf("Failed to add to Queue\n");
        return;
    }
    //tmp->task = (task_t*) malloc(sizeof(task_t));

    tmp->task = value;
    tmp->next = NULL;

    pthread_mutex_lock(&q->tail_lock);
    q->tail->next = tmp;
    q->tail = tmp;
    pthread_cond_signal(&q->conditionVariable);
    pthread_mutex_unlock(&q->tail_lock);
    

}

int dequeue(queue_t * q, task_t* value){
    pthread_mutex_lock(&q->head_lock);
    node_t* tmp = q->head;
    node_t* new_head = tmp->next;
    if(new_head == NULL){
        pthread_mutex_unlock(&q->head_lock);
        return -1; // queue was empty. 
    }

    *value = *new_head->task;
    q->head = new_head;
    pthread_mutex_unlock(&q->head_lock);
    free(new_head->task);
    free(tmp);
    return 0;
}
int peak(queue_t* q, task_t* t){
  pthread_mutex_lock(&q->head_lock);
  node_t* tmp = q->head;
  node_t* new_head = tmp->next;
  if(new_head == NULL){
    pthread_mutex_unlock(&q->head_lock);
    return -1; // check to make sure the queue isn't empty
  }
  t = q->head->task;
  pthread_mutex_unlock(&q->head_lock);
  return 0;
}
void destroyQueue(queue_t* q){
  int rc = 0;
  while(rc != -1){
    task_t* t = (task_t*) malloc(sizeof(task_t));
    rc = peak(q, t);
    rc = dequeue(q, t);
    free(t);
  }
  free(q);
}

 // ==================================================================================================
 // SORTING ALGORITHM 
void merge(Record* input, int start, int midpoint, int end){ // takes a pointer to an array of records.
    int i, j, k;
    int leftHalfLength = midpoint - start + 1;
    int rightHalfLength = end - midpoint;
    // create temporary arrays for sorting. 
    Record* leftArray = malloc(sizeof(Record) * leftHalfLength);
    Record* rightArray = malloc(sizeof(Record) * rightHalfLength);
    leftArray = memset(leftArray, 0, sizeof(Record) * leftHalfLength);
    rightArray = memset(rightArray, 0, sizeof(Record) * rightHalfLength);
// copy over the data
    for(i = 0; i < leftHalfLength; i ++){
      
       leftArray[i] = input[start + i];
    }
    for(j = 0; j < rightHalfLength; j++){
        rightArray[j] = input[midpoint + 1 + j];
    }
    // merge the temporary arrays back
    i = 0; 
    j = 0;
    k = start;
    while(i < leftHalfLength && j < rightHalfLength){
        if(leftArray[i].key <= rightArray[j].key){
            input[k] = leftArray[i];            
            i++;
        }
        else{
            input[k] = rightArray[j];
            j++;
       }
        k++;
    }
    while(i < leftHalfLength){
        input[k] = leftArray[i];
        i++;
        k++;
    }
    while(j < rightHalfLength){
        input[k] = rightArray[j];
        j++;
        k++;
    }
    free(rightArray);
    free(leftArray);
}



void mergeSort(Record*  input, int start, int end){ // start refers to array index "zero", end refers to the index of
// the last element of the array. On an initial call to mergeSort, start should be zero, and end should be array.length - 1
// this method splits the array until it cannot be split anymore, at which the point the work of sorting the split arrays is 
// carried out by merge. 


     if(start < end){
        int midpoint = start + (end - start) / 2;
        mergeSort(input, start, midpoint);
        mergeSort(input, midpoint + 1, end);
        merge(input, start, midpoint, end);
    }
}

// methods
// ===================================================================================================
// THREADING CODE
void* thread(void* arg){
 
    args_t* args = (args_t*) arg;
    
    task_t* task = (task_t*) malloc(sizeof(task_t));
    
    // acquire the lock
    pthread_mutex_lock(&args->queue->tail_lock);
    while( dequeue(args->queue, task) == -1){ // check for work
        pthread_cond_wait(&args->queue->conditionVariable, &args->queue->tail_lock); // wait until work arrives
    }
    pthread_mutex_unlock(&args->queue->tail_lock);
    // task has been removed and we can begin sorting
    int leftBoundary = task->leftBoundary;
    int rightBoundary = task->rightBoundary;
    mergeSort(args->array, leftBoundary, rightBoundary);
    free(task);
    return NULL;
}



void printQueue(queue_t* q){
    queue_t* tempQueue = (queue_t*) malloc(sizeof(queue_t));
    if(tempQueue == NULL){
        printf("failed to allocate tempqueue while printing queue");
        return;
    }

    initializeQueue(tempQueue);
    task_t* tempTask = (task_t*) malloc(sizeof(task_t));
    int dequeueStatus = 0;
    // print the queue
    while( 1 == 1){
       
        dequeueStatus = dequeue(q, tempTask);
       if(dequeueStatus == -1){
            break;
         }

        //printf("Task: [%d, %d]\n", tempTask->leftBoundary, tempTask->rightBoundary);
        enqueue(tempQueue, tempTask);
        
    }
    // repopulate the queue
    dequeueStatus = 0;
    while(1 == 1){
        dequeueStatus = dequeue(tempQueue, tempTask);
        if(dequeueStatus == -1){
            break;
        }
        enqueue(q, tempTask);
    }
    free(tempQueue);
    free(tempTask);
    tempTask = NULL;
    tempQueue = NULL;
}


// ===================================================================================================


// LOCKS
// is any of this even necessary?
/*

typedef struct{
    pthread_mutex_t mutex;
    pthread_cond_t conditionVariable;
    //bool flag;
} lock_t;


lock_t* allocateLock(){
    lock_t* lock = (lock_t*) malloc(sizeof(lock_t));
    if(lock == NULL){
        printf("Warning: failed to create lock. Program should exit.\nReason: %s\n", strerror(errno));
        return NULL;
    }
    return lock;
}

int initialize(lock_t* lock){
    int returnCode = 0;
    if( (returnCode = pthread_mutex_init(&lock->mutex, NULL) != 0)){
        return returnCode;
    }
    if( (returnCode = pthread_cond_init(&lock->conditionVariable, NULL) != 0)){
        return returnCode;
    }
    lock->flag = 0;
    return returnCode;
}

void lock(lock_t* lock){
    pthread_mutex_lock(lock->mutex);
    while(lock->flag){
        pthread_cond_wait(lock->conditionVariable, lock->mutex);
    }
    lock->flag = true;
    pthread_mutex_unlock(lock->mutex);
}

void unlock(lock_t* lock){
  */  

int main(int argc, char** argv){
    
// merge sort testing
/*
    int array[] = 
       {5,121,43,7,56342,8745,9,21,34,134,5321,542123,3232,1,345,32135,54,32,90,76,876,7432,5432,4523,5432,6421,543,1,2,541,432};
   mergeSort(array, 0, 30);
   for(int i = 0; i < 30; i++){
       printf("%i\n", array[i]);
   }
// end merge sort testing
*/
	FILE* in_file = NULL;
	FILE* out_file = NULL;
	int timed = -1;
// ===================================================================================================
// argument parsing
    if(argc < 4 || argc > 4){
        printf("usage: psort (-p) [input] [output]\nUse '-p' for timing information. It will be written to stdout\n");
        return 0;
    }
    else if(strcmp(argv[2], "-p")){
	in_file = fopen(argv[2], "r");
	out_file = fopen(argv[3], "w+"); 
    
	timed = 1;
    }
    	else{
        printf("Using arguments: %s and %s\n",argv[1], argv[3]);

    	in_file = fopen(argv[1], "rb");
    	out_file = fopen(argv[2], "wb");
    	timed = 0;
	}
// READ PHASE
// ===================================================================================================
    //FILE* in_file = fopen(argv[1], "r");
    //FILE* out_file = fopen(argv[2], "w+");

    if( (in_file == NULL|| out_file == NULL)){
        printf("Error opening file: %s\n", strerror(errno));
        return 0;
    }
    // timing setup
    struct timeval start_read_phase;
    struct timeval end_read_phase;
    if(timed == 1){
    	gettimeofday(&start_read_phase, NULL);
    }
    // set up for file reading. Allocate for a buffer, length variable
     
    const size_t BUFFER_SIZE = RECORD_SIZE * 40; 
    uint8_t* buffer = (uint8_t*) malloc(sizeof(uint8_t) * BUFFER_SIZE);
    if(buffer == NULL){
        printf("failed to allocate buffer for file reading. aborting. \n");
        return -1;
    }

// ===================================================================================================
    // create structs for holding records. 
    // Initially, this array will have capacity for double the number of records the file input buffer 
    // has capacity for. 
    size_t recordArraySize = BUFFER_SIZE * 2; // deprecate
    size_t recordArrayLength = sizeof(Record) * ((BUFFER_SIZE * 2) / RECORD_SIZE);
    
    Record* recordArray = (Record*) malloc(sizeof(Record) * recordArrayLength);
    

    if(recordArray == NULL){
        printf("Error allocating memory for Record Array. Aborting.\n");
        return -1;
    }

    buffer = memset(buffer, 0, BUFFER_SIZE);
    recordArray = memset(recordArray, 0, sizeof(Record) * recordArrayLength);
// ===================================================================================================
    clearerr(in_file);
    size_t recordArrayIndex = 0;
    int total_bytes_read = 0; 
    int current_bytes_read = -1;
    //recordArray->length = recordArraySize / RECORD_SIZE; // recordArraySize / RECORD_SIZE = 128*40 * 2 / 128
    // core of file reading and writing. 
    while((current_bytes_read =  fread(buffer, sizeof(uint8_t), BUFFER_SIZE, in_file)) != 0){
        total_bytes_read += current_bytes_read;
         if(ferror(in_file) != 0){
             printf("An error occurred while reading the input file: %s\n", strerror(errno));
             return -1;
         }
         if(recordArrayIndex >= recordArrayLength - 1){ // we have more records in the input file, 
                                                      // but not enough space in the record array.
                                                     // resize the record array so we can keep reading. 
            size_t oldSize = recordArrayLength * RECORD_SIZE;
            size_t newSize = oldSize * 2;
            
      //      printf("resizing the record array. Old size: %zd | New size: %zd\n", oldSize, newSize);
            recordArray = (Record*) realloc(recordArray,sizeof(Record) + newSize);
            recordArrayLength = newSize / RECORD_SIZE;
            // TODO figure out how to memset the new region of memory. What is the correct pointer arithmetic?
            // memset(recordArray + oldSize, 0, newSize); ???

         }
    //     int items_read_count = fread(buffer, sizeof(uint8_t), BUFFER_SIZE, in_file);
    
        // this is where the "too much data is being written" issue lies, this is being ran one too many times, and so 
        // recordArrayIndex is being incremented 20 past the actual value of how many records have been read. 
        for(size_t buffer_index  = 0; buffer_index  < BUFFER_SIZE / sizeof(Record); buffer_index++){
          
            if( (uintptr_t)buffer + sizeof(Record) * buffer_index > (uintptr_t) buffer +  current_bytes_read){
                // my attempt at stopping copying memory
                // when the buffer is not completely full of Records. Will NOT work because 
                // buffer + sizeof(record) * buffer_index is always greater than current_bytes_read 
                break;
            }
            /*
            Record* record = (Record*) malloc(sizeof(Record));
            if(record == NULL){
                printf("Failed to allocate memory for record: %s\n", strerror(errno));
                return -1;
            }
            */

            //record->key = memcpy(&record->key, buffer);
             memcpy(&(recordArray[recordArrayIndex]), buffer + sizeof(Record) * buffer_index, sizeof(Record));
//           printf("Record: %d | Key: %" PRIu32 "\n", recordArrayIndex, recordArray[recordArrayIndex].key); 
           recordArrayIndex++; 
         
         }
    }
// END READ PHASE
// ==========================================================================================================================
    if(timed == 1){
    
    	gettimeofday(&end_read_phase, NULL);
    long seconds  = end_read_phase.tv_sec  - start_read_phase.tv_sec;
    long useconds = end_read_phase.tv_usec - start_read_phase.tv_usec;
    double elapsed  = seconds + useconds/1e6;

    //printf("Elapsed time: %.6f seconds\n", elapsed);
    	printf("{\"Records\": %d}\n", total_bytes_read / 128);
    	printf("{\"Read Phase Time Elapsed\": %f}\n", elapsed); 
    	
    }
    //printf("Total Bytes Read: %d\n", total_bytes_read);
    // sort the records. 
 // the issue is I have a bunch of empty space in the tail of the record array, because not all of it is used for records.
 //  because all of their keys are 0, they are automatically all sorted to the bottom first. 
 //  i need to either set all those keys to some "max key value" 
 //     or reduce the size of the record array once I am done to only fit the valid region that is filled with records, not junk data. 
 //     -- going with option 2 --
 //  realloc, calculate the lowest address of first piece of junk data
 recordArray = realloc(recordArray, sizeof(Record) * recordArrayIndex); // how can i be sure i am not overwriting data?       
    recordArrayLength = recordArrayIndex + 1;
// ==============================================================================================================================
    // queue initializtion and testing. 
    
// TASK CREATION AND THREAD SPIN UP PHASE
    queue_t* queue = (queue_t*) malloc(sizeof(queue_t));
    initializeQueue(queue);
    
    // basic queue tests
    /*
    task_t* task1 = (task_t*)  malloc(sizeof(task_t));
    task1->leftBoundary = 0;
    task1->rightBoundary = 5;
    enqueue(queue, task1);
    
    task_t* task2 = (task_t*)  malloc(sizeof(task_t));
    task2->leftBoundary = 20;
    task2->rightBoundary = 10;
    enqueue(queue, task2);



    task_t* task3 = (task_t*)  malloc(sizeof(task_t));
    task3->leftBoundary = 4;
    task3->rightBoundary = 3;
    enqueue(queue, task3);



    printQueue(queue);
    free(task1);
    free(task2);
    free(task3);
    */

    // task creation (chunking work)
    // we will use a "concurrency degree" to determine how many chunks (tasks) to create, and the boundaries of the chunks.
    // We will spin up an equal number of threads. 
    // The concurrency degree will be decided based on how large the array is. Arbitrarily, I am deciding that each thread
    // should service about 100 records. 
    // I should threshold the concurrency degree. When the file size is smaller than X bytes, we need less threads
    // when the file size is larger than X bytes, we need more threads. 
    // A roughly 6KB file contains about 50 records. I would say, when the file contains less than 100 records,
    // two threads ought to be more than enough, and we might even run into the performance overhead of spinning up these threads. 
    // This concurrency degree will have to be decided via trial and error. I will run tests to see where I get the most benefits. 
    // thread count will be determined as follows: 
    // min(core count, ceiling(size of data / chunk size))
    // since this is all integers, i don't need to bother with the cieling function
    // chunk size needs to be figured ou:
    //  - what kind of workload makes threading "worth it"? 
    //    At what point is the cost of spinning up threads, context switching, and the final merge worth it? 
    //

    //compute concurrency degree
    // 1. Compute the ceiling of the amount of data divided by the size of the
    // chunk of work each thread will be responsible for 

   int concurrencyDegree = 0;
   //int chunk_size = 1000; // by default, each thread will be responsible for sorting 10,000 records.
   // need to see if there's a better value. 
   //concurrencyDegree = recordArrayLength / chunk_size;

   
    // ensuring thread count is no more than CPU core count. 

    cpu_set_t cpuset;
    int num_cores = 0;
    sched_getaffinity(0, sizeof(cpu_set_t), &cpuset);
    num_cores = CPU_COUNT(&cpuset);
    concurrencyDegree = num_cores;
    //if(num_cores < concurrencyDegree){
      //  concurrencyDegree = num_cores;
       // printf("Calculated concurrency degree exceeded system CPU core count.\nAutomatically adjusted degree to match core count.\n Thread count is maxed at: %d\n", concurrencyDegree);
    //}

            

    // create tasks, make threads, perform sort on the tasks. 

    int i = 0;
    //int number_of_chunks = recordArrayLength / chunk_size;
    //printf("Workload divided into: %d chunks.\n", number_of_chunks);

    // create tasks, push onto queue;
    while(i < concurrencyDegree){
        // tasks are leaked. this will be solved by creating a DESTROY QUEUE method that frees all nodes and tasks associated. 
        // Consider that each thread has access to the pointer to their task object, they can potentially free the memory after they are done sorting. 
        task_t* task = (task_t*) malloc(sizeof(task_t));
        task->leftBoundary = ( recordArrayLength / concurrencyDegree) * i;
        task->rightBoundary = (( recordArrayLength / concurrencyDegree) * (i +1)) - 1;
        int j = task->rightBoundary - task->leftBoundary;
        //printf("Size of Task# %d:%d\n", i, task->rightBoundary - task->leftBoundary);
        enqueue(queue, task);
        i = i + 1;
    }

    // create threads
    // threads leaks, TODO free appropriately
    pthread_t* threads = (pthread_t*) malloc(sizeof(pthread_t) * concurrencyDegree);
    int* threadIDs = (int*) malloc(sizeof(int) * concurrencyDegree);
    args_t* args_array = (args_t*) malloc(sizeof(args_t) * concurrencyDegree);

    if(threads == NULL || threadIDs == NULL){
        printf("failed to allocate thread array or thread ID array.\n");
        // TODO free everything
        free(queue);
        free(recordArray);
        free(buffer);
            
        if(fclose(in_file) != 0 || fclose(out_file) != 0){
            printf("%s\n", strerror(errno));
        }
        return -1;
    }
    
    
 // SORT REGION
 // ================================================================================================================================
    // spin up threads
    struct timeval start_sort_phase, end_sort_phase;
    if( timed == 1){
	    gettimeofday(&start_sort_phase, NULL);;
    }
    for(int i = 0; i < concurrencyDegree; i++){
        // args leaks. TODO free appropriately

        
        args_array[i].threadID = i;
        args_array[i].array = recordArray;
        args_array[i].queue = queue;
        int rc =  pthread_create(&threads[i], NULL, thread, &args_array[i]);
        if(rc != 0){
            printf("Failed to create a thread for the following reason.\n%s\n", strerror(rc));
            // TODO signifies the sorting will not go to plan, and the program should perform cleanup and report the inability to complete the task
           
	    return -1;
        }
    }
    // the main thread should wait for all the other threads to finish sorting before performing the final merge, writing to out, 
    // and performing cleanup. Join on all threads. 
    for( int i = 0; i < concurrencyDegree; i++){
        pthread_join(threads[i], NULL);
    }
    free(args_array);
// END SORT REGION
// ===============================================================================================================================
    if(timed == 1){
	gettimeofday(&end_sort_phase, NULL);
    long seconds  = end_sort_phase.tv_sec  - start_sort_phase.tv_sec;
    long useconds = end_sort_phase.tv_usec - start_sort_phase.tv_usec;
    double elapsed  = seconds + useconds/1e6;

    //printf("Elapsed time: %.6f seconds\n", elapsed);
    	printf("{\"Sort Phase Elapsed Time\": %f}\n", elapsed);
    }	
// threads no longer needed, free here. 
    free(threads);
    free(threadIDs);
    // merge  the chunks. 
    // perform a serial merge. 

    //mergeSort(recordArray, 0, recordArrayLength - 1);
    
    // print for debugging


// serial merge
//
// no need for synchronization at this step, the only thread that exists is this one. 
// MERGE AND WRITE REGION
// ===========================================================================================================================
    
   struct timeval start_merge_phase, end_merge_phase;
  if( timed == 1){
	 gettimeofday(&start_merge_phase, NULL);
  } 
    int chunk_size = recordArrayLength / concurrencyDegree;
for (int step = 1; step < concurrencyDegree; step *= 2) {
    for (int i = 0; i + step < concurrencyDegree; i += 2 * step) {
        int left = i * chunk_size;
        int mid = (i + step) * chunk_size;
        int right = (i + 2 * step < concurrencyDegree ? (i + 2 * step) : concurrencyDegree) * chunk_size;
        //printf("segfault?\n");
  // chatgpt recommended the following  if statement as a fix to an issue where, on occasion, 
  // records would be out of order in the final merged output, but only in the last chunk, and only a very small number of records.  	
	// this is here to ensure that 'right' never exceeds the bounds of the array. chunk_size may not divide eavenly because of a recordArrayLength that doesn't get divided cleanly by concurrencyDegree.
	// Since 'right' is tied to chunk_size, right needs to be curtailed in case of an overrun of bounds. 
	if(right >= recordArrayLength){
		right = recordArrayLength -1;
	}
	
 	merge(recordArray, left, mid, right);
    }
}

    
    //printf("record array index:  %zd\n", recordArrayIndex);
/*   
for(size_t i = 0; i < recordArrayLength; i+= 1 * 10000){
       printf("Record:%zd | Key: %" PRIu32 "\n",i, recordArray[i].key);
    }
  */  

    // write to file
    int items_written_count = fwrite(recordArray, sizeof(Record), recordArrayIndex - 1, out_file);
    //printf("Total Records Written: %d\n", items_written_count);
    // close streams, exit
    int writeFileDescriptor = fileno(out_file);
    int fsyncrc = fsync(writeFileDescriptor);  
    if(fsyncrc!= 0){
	    printf("error flushing output file\n%s\n", strerror(errno));
    }
    
// END MERGE AND WRITE REGION
    if( timed == 1){
	 gettimeofday(&end_merge_phase, NULL);
    long seconds  = end_merge_phase.tv_sec  - start_merge_phase.tv_sec;
    long useconds = end_merge_phase.tv_usec - start_merge_phase.tv_usec;
    double elapsed  = seconds + useconds/1e6;

    //printf("Elapsed time: %.6f seconds\n", elapsed);
      printf("{\"Merge and Write Phase Time Elapsed\": %.6f}\n",elapsed);	 
    }
      
    if(fclose(in_file) != 0 || fclose(out_file) != 0){
	printf("%s\n", strerror(errno));
    }
// free allocated memory
    free(buffer);
    free(recordArray);
    //free(queue);
    destroyQueue(queue); 
    return 0;
 }

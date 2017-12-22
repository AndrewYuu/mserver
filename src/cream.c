#include "cream.h"
#include "queue.h"
#include "utils.h"
#include "debug.h"

#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <signal.h>

queue_t *request_queue;
hashmap_t *data;

void destroy_function(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}

void *thread(void *vargp){
    while(1){
        //WORKER THREADS WILL WAIT/BE BLOCKED UNTIL THERE IS A JOB REQUEST ON THE QUEUE TO DO.
        //NO REQUESTS MEANS NOTHING TO DECQUEUE.
        int *connfdp = (int *)dequeue(request_queue); //connfdp IS LIKE A PIPE.
        debug("In thread routine, connfdp fron dequeue is %d", *connfdp);
        debug("In thread routine");

        if(errno == EPIPE){
            close(*connfdp);
            continue;
        }

        request_header_t requestHeader;
        recv(*connfdp, &requestHeader, sizeof(requestHeader), 0);

        if(errno == EINTR){
            exit(1);
        }

        response_header_t responseHeader;

        debug("CODE: %d\nKEY SIZE: %d\nVAL SIZE: %d\n", requestHeader.request_code, requestHeader.key_size, requestHeader.value_size);


        // IT IS A file WHERE READING FROM connfdp WOULD OBTAIN THE REQUEST FROM THE CLIENT. WRITING TO connfdp WOULD WRITE TO THE CLIENT.
        // FIRST PARSE THE requestHeader INTO THE BUFFER. READ IN requestHeader SIZE BYTES.

        debug("Request code: %d", requestHeader.request_code);

        //IF CLIENT SENDS MSG TO SERVER AND request_code IS NOT SET TO ANY OF THE ENUM CODES.
        if(requestHeader.request_code == 0 || (requestHeader.request_code != PUT && requestHeader.request_code != GET && requestHeader.request_code != EVICT && requestHeader.request_code != CLEAR)){
            responseHeader.response_code = UNSUPPORTED;
            responseHeader.value_size = 0;
            send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
        }

        //WORK (MODIFYING THE DATA STRUCTURE) BY READING THE REQUEST FROM THE CONNFDP DEQUEUED FROM THE QUEUE.

        if(requestHeader.request_code == PUT){
            //NEXT, PARSE THE KEY VALUE BY recv FROM connfdp INTO THE BUFFER WITH key_size BYTES.
            //AFTERWARDS, PARSE THE VAL VALUE BY recv FROM connfdp INTO THE BUFFER WITH value_size BYTES.
            //NOT PARSING THE STRING USER TYPES INTO THE CLIENT (i.e: "put 0 1") SINCE THE CLIENT PARSES THE STRING INTO
            //A SEQUENCE OF BYTES THAT IS requestHeader FOLLOWED BY keyvalue AND valvalue BEFORE SENDING IT TO THE SERVER.
            if(requestHeader.key_size > MAX_KEY_SIZE || requestHeader.key_size < MIN_KEY_SIZE
                || requestHeader.value_size > MAX_VALUE_SIZE || requestHeader.value_size < MIN_VALUE_SIZE){
                // send(*connfdp, "Error Bad Request 400", 100, 0); //SEND ERROR MESSAGE RESPONSE HEADER OF BAD_REQUEST
                //SEND ERROR MESSAGE BAD REQUEST
                responseHeader.response_code = BAD_REQUEST;
                responseHeader.value_size = requestHeader.value_size;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
            }

            debug("Thread puts");
            //PUT
            char *keyBuff = calloc(1, requestHeader.key_size);
            char *valBuff = calloc(1, requestHeader.value_size);
            recv(*connfdp, keyBuff, requestHeader.key_size, 0);

            if(errno == EINTR){
                exit(1);
            }

            recv(*connfdp, valBuff, requestHeader.value_size, 0);

            if(errno == EINTR){
                exit(1);
            }

            debug("READ KEY: %s\nREAD VALUE: %s\n", keyBuff, valBuff);

            //DO NOT CALLOC THE STRUCTS BECAUSE THEY ARE ALREADY ALLOCATED SPACE IN MEMORY ON THE STACK. DOES NOT NEED TO BE
            //ON THE HEAP BECAUSE IT DOES NOT NEED TO BE MODIFIED AND RETURNED BY ANOTHER FUNCTION.
            map_key_t map_key;
            map_key.key_base = keyBuff;
            map_key.key_len = requestHeader.key_size;


            map_val_t map_val;
            map_val.val_base = valBuff;
            map_val.val_len = requestHeader.value_size;

            debug("Key value: %d", *(int *)(map_key.key_base));
            debug("Key size: %d", (int) map_key.key_len);
            debug("Val value: %d", *(int *)(map_val.val_base));
            debug("Val size: %d", (int) map_val.val_len);
            bool putResult = put(data, map_key, map_val, 1);

            if(putResult == false){
                //RESPOND TO CLIENT BAD REQUEST, AND RESPONSE HEADER VALUE SIZE TO 0
                //SEND ERROR MESSAGE BAD REQUEST
                responseHeader.response_code = BAD_REQUEST;
                responseHeader.value_size = 0;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
            }
            else{
                //RESPOND TO CLIENT OK WITH VALUE SIZE
                responseHeader.response_code = OK;
                responseHeader.value_size = requestHeader.value_size;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
            }

        }
        if(requestHeader.request_code == GET){
            //PARSE THE BUFFER AND GET FROM HASHMAP
            if(requestHeader.key_size > MAX_KEY_SIZE || requestHeader.key_size < MIN_KEY_SIZE){
                // send(); //SEND ERROR MESSAGE RESPONSE HEADER OF BAD_REQUEST
                responseHeader.response_code = BAD_REQUEST;
                responseHeader.value_size = requestHeader.value_size;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
            }

            debug("Thread gets");
            //GET
            char *keyBuff = calloc(1, requestHeader.key_size);
            recv(*connfdp, keyBuff, requestHeader.key_size, 0);

            if(errno == EINTR){
                exit(1);
            }

            debug("READ KEY: %s\n", keyBuff);

            map_key_t map_key;
            map_key.key_base = keyBuff;
            map_key.key_len = requestHeader.key_size;


            debug("Key value: %d", *(int *)(map_key.key_base));
            debug("Key size: %d", (int) map_key.key_len);

            map_val_t getValue = get(data, map_key);
            if(getValue.val_base == NULL){
                debug("Send response code not found.");
                //SEND TO CLIENT RESPONSE CODE NOT FOUND
                responseHeader.response_code = NOT_FOUND;
                responseHeader.value_size = 0;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
            }
            else{
                debug("send response code found.");
                //SEND TO CLIENT RESPONSE CODE OK, AND THE VALUE SIZE IN BYTES OF THE CORRESPONDING VALUE FROM GET.
                responseHeader.response_code = OK;
                responseHeader.value_size = getValue.val_len;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
                send(*connfdp, getValue.val_base, getValue.val_len, 0);
            }

        }
        if(requestHeader.request_code == EVICT){
            //PARSE THE BUFFER AND EVICT FROM HASHMAP
            if(requestHeader.key_size > MAX_KEY_SIZE || requestHeader.key_size < MIN_KEY_SIZE){
                // send(); //SEND ERROR MESSAGE RESPONSE HEADER OF BAD_REQUEST
                responseHeader.response_code = BAD_REQUEST;
                responseHeader.value_size = requestHeader.value_size;
                send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
            }
            //DELETE
            void *keyBuff = calloc(1, requestHeader.key_size);
            recv(*connfdp, keyBuff, requestHeader.key_size, 0);

            if(errno == EINTR){
                exit(1);
            }

            map_key_t map_key;
            map_key.key_base = keyBuff;
            map_key.key_len = requestHeader.key_size;

            delete(data, map_key);
            responseHeader.response_code = OK;
            responseHeader.value_size = 0;
            send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
        }
        if(requestHeader.request_code == CLEAR){
            //PARSE THE BUFFER AND CLEAR HASHMAP
            clear_map(data);
            responseHeader.response_code = OK;
            responseHeader.value_size = 0;
            send(*connfdp, &responseHeader, sizeof(responseHeader), 0);
        }
        close(*connfdp);
    }
    //RESPOND TO CLIENT
    //RETURN
    return NULL;
}

int open_listenfd(char *port){
    int listenfd, optval=1;
    struct sockaddr_in serveraddr;

    /* Create a socket descriptor */
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        return -1;

    /* Eliminates "Address already in use" error from bind */
    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (const void *)&optval , sizeof(int)) < 0)
        return -1;

    /* Listenfd will be an endpoint for all requests to port
       on any IP address for this host */
    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(atoi(port));
    if (bind(listenfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) < 0)
        return -1;

    /* Make it a listening socket ready to accept connection requests */
    if (listen(listenfd, 1024) < 0)
        return -1;
    return listenfd;
}

void printhelp(){
    printf("./cream [-h] NUM_WORKERS PORT_NUMBER MAX_ENTRIES\n-h                 Displays this help menu and returns EXIT_SUCCESS.\nNUM_WORKERS        The number of worker threads used to service requests.\nPORT_NUMBER        Port number to listen on for incoming connections.\nMAX_ENTRIES        The maximum number of entries that can be stored in `cream`'s underlying data store.");
}

int main(int argc, char *argv[]) {

    if(strncmp(argv[1], "-h", 2) == 0){
        printhelp();
        exit(0);
    }

    if(argc != 4){
        exit(1);
    }

    signal(SIGPIPE, SIG_IGN);

    int numberOfWorkers = atoi(argv[1]);
    int maxEntries = atoi(argv[3]);
    pthread_t worker_threads[numberOfWorkers];
    request_queue = create_queue();
    data = create_map(maxEntries, jenkins_one_at_a_time_hash, destroy_function);

    int listenfd = 0;
    int *connfdp = 0; //LINK BETWEEN SERVER AND CLIENT. WORKER THREADS RUN THIS TO MODIFY HASHMAP

    socklen_t clientlen;
    struct sockaddr_storage clientaddr;

////////SPAWN WORKER THREADS//////
    //WORKER THREAD SPAWNING. FOR INTERACTION WITH HASHMAP DATA.
    //ONCE THE THREADS ARE SPAWNED, THEY IMMEDIATELY GET SCHEDULED TO RUN THEIR THREAD ROUTINE.
    //HOWEVER, IF THERE ARE NO REQUESTS ON THE QUEUE, THE THREAD WILL STAY WAITING IN THE THREAD ROUTINE
    //UNTIL IT CAN DEQUEUE.
    for(int i = 0; i < numberOfWorkers; i++){
        pthread_create(&worker_threads[i], NULL, thread, data);
    }

////////SET UP SERVER/////// SIMPLY LISTENS AND ACCEPTS

    listenfd = open_listenfd(argv[2]);
    //ACCEPT AWAITING REQUESTS ONE AT A TIME AND ENQUEUE TO THE QUEUE.
    while(1){
        clientlen = sizeof(struct sockaddr_storage);
        connfdp = malloc(sizeof(int)); //SO THAT connfdp IS NOT SHARED ON THE STACK BETWEEN THREADS.
        *connfdp = accept(listenfd, (struct sockaddr*)&clientaddr, &clientlen);
        //ADD ACCEPTED SOCKET listenfd TO QUEUE. ENQUEUE
        debug("In main thread: Connfdp is %d", *connfdp);
        enqueue(request_queue, connfdp);
    }

}


//EPIPE IS AN errno. CLOSE THE CONNECTION ONCE errno IS EPIPE.
//SIGPIPE SHOULD BE SIGIGN. SHOULD BE IGNORED IN THE MAIN THREAD.
//EINTR IS AN errno IS AN INTERRUPT DURING WRITE. DURING THE THREAD ROUTINE, IF THIS
//IS ENCOUNTERED, WRITE SHOULD BE DONE AGAIN.

//SIGIGN SIGPIPE. THEN CHECK ERRNO FOR EPIPE. IF EPIPE, close(connfdp);
